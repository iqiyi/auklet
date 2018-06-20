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

package objectserver

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
	"github.com/iqiyi/auklet/common/ring"
	"github.com/iqiyi/auklet/common/srv"
)

type Updater struct {
	logger      *zap.Logger
	devices     map[int][]string
	asyncJobMgr AsyncJobMgr
	cRing       ring.Ring
	prefix      string
	suffix      string
	concurrency int
	srvPort     int
	client      *http.Client
}

func (u *Updater) updateContainer(job AsyncJob) bool {
	successes := 0
	header := common.Map2Headers(job.GetHeaders())
	header.Set(common.HUserAgent, fmt.Sprintf("object-updater %d", os.Getpid()))

	partition := u.cRing.GetPartition(job.GetAccount(), job.GetContainer(), "")
	for _, node := range u.cRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s",
			node.Ip,
			node.Port,
			node.Device,
			partition,
			common.Urlencode(job.GetAccount()),
			common.Urlencode(job.GetContainer()),
			common.Urlencode(job.GetObject()))
		req, err := http.NewRequest(job.GetMethod(), url, nil)
		if err != nil {
			u.logger.Error("unable to creating new request", zap.Error(err))
			continue
		}
		req.Header = header
		resp, err := u.client.Do(req)
		if err != nil {
			u.logger.Error("unable to update container", zap.Error(err))
			continue
		}
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			successes++
		}
	}

	return successes >= (int(u.cRing.ReplicaCount()/2) + 1)
}

func (u *Updater) updateDevice(
	policy int, device string, pool chan bool, wg *sync.WaitGroup) {
	defer func() {
		<-pool
		wg.Done()
	}()

	u.logger.Info("begin to update device",
		zap.String("device", device), zap.Int("policy", policy))
	job := u.asyncJobMgr.Next(device, policy)
	for ; job != nil; job = u.asyncJobMgr.Next(device, policy) {
		if !u.updateContainer(job) {
			u.logger.Info("unable to update container")
			continue
		}

		u.logger.Debug("container got updated", zap.Any("job", job))

		if err := u.asyncJobMgr.Finish(job); err != nil {
			u.logger.Error("unable to cleanup pending job", zap.Error(err))
		}
	}

	u.logger.Info("device updated",
		zap.String("device", device), zap.Int("policy", policy))
}

func (u *Updater) update() {
	pool := make(chan bool, u.concurrency)
	wg := &sync.WaitGroup{}

	for p, devs := range u.devices {
		for _, d := range devs {
			pool <- true
			wg.Add(1)
			go u.updateDevice(p, d, pool, wg)
		}
	}

	wg.Wait()
}

func (u *Updater) Run() {
	u.update()
}

func (u *Updater) RunForever() {
	u.logger.Info("running object updater forever")

	for {
		u.update()
		time.Sleep(time.Second * 10)
	}
}

func (u *Updater) parseConf(cnf conf.Config) {
	u.srvPort = int(cnf.GetInt("app:object-server", "bind_port", 6000))
	u.concurrency = int(cnf.GetInt("object-updater", "concurrency", 1))
}

func (u *Updater) listDevices(policyFilter, deviceFilter string) {
	pf := map[int]bool{}
	for _, p := range strings.Split(policyFilter, ",") {
		if p == "" {
			continue
		}

		pi, err := strconv.Atoi(p)
		if err != nil {
			u.logger.Error("unable to parse policy filter, ignore",
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
		if len(pf) > 0 && !pf[policy.Index] {
			continue
		}

		devs, err := ring.ListLocalDevices(
			"object", u.prefix, u.suffix, policy.Index, u.srvPort)
		if err != nil {
			u.logger.Error("unable to get local device list",
				zap.Int("policy", policy.Index),
				zap.Int("port", u.srvPort),
				zap.Error(err))
			continue
		}

		for _, d := range devs {
			if len(df) == 0 || df[d.Device] {
				devices[policy.Index] = append(devices[policy.Index], d.Device)
			}
		}
	}

	u.devices = devices
}

func InitUpdater(cnf conf.Config, flags *flag.FlagSet) (srv.Daemon, error) {
	logger, err := common.GetLogger(
		flags.Lookup("l").Value.(flag.Getter).Get().(string), "object-updater")
	if err != nil {
		return nil, err
	}

	glogger = logger

	u := &Updater{
		logger: logger,
		client: &http.Client{Timeout: 5 * time.Minute},
	}

	u.asyncJobMgr, err = NewAsyncJobMgr(cnf, flags)
	if err != nil {
		return nil, err
	}
	u.prefix, u.suffix, err = conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, err
	}

	u.cRing, err = ring.GetRing("container", u.prefix, u.suffix, 0)
	if err != nil {
		return nil, err
	}

	u.parseConf(cnf)

	policyFilter := flags.Lookup("policies").Value.(flag.Getter).Get().(string)
	deviceFilter := flags.Lookup("devices").Value.(flag.Getter).Get().(string)
	u.listDevices(policyFilter, deviceFilter)

	return u, nil
}
