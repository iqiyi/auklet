// Copyright (c) 2016-2018 iQIYI.com.  All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// 

package pack

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
	"github.com/iqiyi/auklet/common/fs"
	"github.com/iqiyi/auklet/common/ring"
	"github.com/iqiyi/auklet/common/srv"
)

type Auditor struct {
	logger      *zap.Logger
	driveRoot   string
	devices     map[int][]string
	partitions  map[string]bool
	concurrency int
	interval    int
	rpcPort     int
	srvPort     int
	rpc         PackRpcServiceClient
	hashPrefix  string
	hashSuffix  string
}

func (a *Auditor) listPartitions(policy int, device string) []string {
	objPath, _ := PackDevicePaths(device, a.driveRoot, policy)
	suffixes, err := fs.ReadDirNames(objPath)
	if err != nil {
		a.logger.Error("unable to get partition list", zap.Error(err))
		return nil
	}

	var partitions []string
	for _, suff := range suffixes {
		if (len(a.partitions) > 0 && !a.partitions[suff]) ||
			!common.IsDecimal(suff) {
			continue
		}

		partitions = append(partitions, suff)
	}

	return partitions
}

func (a *Auditor) auditDevice(
	policy int, device string, pool chan bool, wg *sync.WaitGroup) {
	defer func() {
		<-pool
		wg.Done()
	}()

	a.logger.Info("begin to audit device",
		zap.String("device", device), zap.Int("policy", policy))

	stat := &AuditStat{}

	for _, p := range a.listPartitions(policy, device) {
		// TODO: shall we need to add a timeout?
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		arg := &Partition{
			Policy:    uint32(policy),
			Device:    device,
			Partition: p,
		}

		reply, err := a.rpc.AuditPartition(ctx, arg)
		if err != nil {
			a.logger.Error("unable to audit partition",
				zap.Int("policy", policy),
				zap.String("device", device),
				zap.String("partition", p),
				zap.Error(err))
			continue
		}

		stat.ProcessedBytes += reply.ProcessedBytes
		stat.ProcessedFiles += reply.ProcessedFiles
		stat.Quarantines += reply.Quarantines
		stat.Errors += reply.Errors
	}

	a.logger.Info("device audited",
		zap.String("device", device),
		zap.Int("policy", policy),
		zap.Int64("bytes", stat.ProcessedBytes),
		zap.Int64("files", stat.ProcessedFiles),
		zap.Int64("errors", stat.Errors),
		zap.Int64("quarantines", stat.Quarantines))
}

func (a *Auditor) audit() {
	pool := make(chan bool, a.concurrency)
	wg := &sync.WaitGroup{}

	for p, devs := range a.devices {
		// TODO: shuffle the devices
		for _, d := range devs {
			pool <- true
			wg.Add(1)
			go a.auditDevice(p, d, pool, wg)
		}
	}

	wg.Wait()
}

func (a *Auditor) Run() {
	a.audit()
}

func (a *Auditor) RunForever() {
	a.logger.Info("running pack auditor forever")

	for {
		a.audit()
		time.Sleep(time.Second * time.Duration(a.interval))
	}
}

func (a *Auditor) parseConf(cnf conf.Config) {
	a.srvPort = int(cnf.GetInt("app:object-server", "bind_port", 6000))
	a.driveRoot = cnf.GetDefault("app:object-server", "devices", "/srv/node")

	a.rpcPort = int(cnf.GetInt("object-auditor", "rpc_port", 60000))
	a.concurrency = int(cnf.GetInt("object-auditor", "concurrency", 1))
	a.interval = int(cnf.GetInt("object-auditor", "interval", 60*60*24*7))
}

func (a *Auditor) listDevices(
	policyFilter, deviceFilter, partitionFilter string) {
	pf := map[int]bool{}
	for _, p := range strings.Split(policyFilter, ",") {
		if p == "" {
			continue
		}

		pi, err := strconv.Atoi(p)
		if err != nil {
			a.logger.Error("unable to parse policy filter, ignore",
				zap.String("policies", policyFilter), zap.Error(err))
			continue
		}

		pf[pi] = true
	}

	df := map[string]bool{}
	for _, d := range strings.Split(deviceFilter, ",") {
		if d != "" {
			df[d] = true
		}
	}

	devices := map[int][]string{}
	for _, policy := range conf.LoadPolicies() {
		if policy.Type != NAME || (len(pf) > 0 && !pf[policy.Index]) {
			continue
		}

		devs, err := ring.ListLocalDevices(
			"object", a.hashPrefix, a.hashSuffix, policy.Index, a.srvPort)
		if err != nil {
			a.logger.Error("unable to get local device list",
				zap.Int("policy", policy.Index),
				zap.Int("port", a.srvPort),
				zap.Error(err))
			continue
		}

		for _, d := range devs {
			if len(df) == 0 || df[d.Device] {
				devices[policy.Index] = append(devices[policy.Index], d.Device)
			}
		}
	}

	a.partitions = map[string]bool{}
	for _, p := range strings.Split(partitionFilter, ",") {
		if p != "" {
			a.partitions[p] = true
		}
	}

	a.devices = devices
}

func InitAuditor(cnf conf.Config, flags *flag.FlagSet) (srv.Daemon, error) {
	logger, err := common.GetLogger(
		flags.Lookup("l").Value.(flag.Getter).Get().(string), "pack-auditor")
	if err != nil {
		return nil, err
	}

	auditor := &Auditor{logger: logger}

	auditor.parseConf(cnf)

	prefix, suffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, ErrHashConfNotFound
	}
	auditor.hashPrefix = prefix
	auditor.hashSuffix = suffix

	policyFilter := flags.Lookup("policies").Value.(flag.Getter).Get().(string)
	deviceFilter := flags.Lookup("devices").Value.(flag.Getter).Get().(string)
	partitionFilter := flags.Lookup("partitions").Value.(flag.Getter).Get().(string)
	auditor.listDevices(policyFilter, deviceFilter, partitionFilter)

	conn, err := grpc.Dial(
		fmt.Sprintf("localhost:%d", auditor.rpcPort), grpc.WithInsecure())
	if err != nil {
		logger.Error("unable to dial to rpc server",
			zap.Int("port", auditor.rpcPort), zap.Error(err))
		return nil, err
	}
	auditor.rpc = NewPackRpcServiceClient(conn)

	return auditor, nil
}
