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
	"io/ioutil"
	"math/rand"
	"net/http"
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
	"github.com/iqiyi/auklet/common/pickle"
	"github.com/iqiyi/auklet/common/ring"
	"github.com/iqiyi/auklet/common/srv"
)

type ReplicationStat struct {
	rehashed   int64
	replicated int64
}

func (s *ReplicationStat) reset() {
	s.rehashed = 0
	s.replicated = 0
}

type Replicator struct {
	logger *zap.Logger
	stat   *ReplicationStat

	driveRoot   string
	concurrency int
	interval    int
	rpcPort     int
	srvPort     int

	rings      map[int]ring.Ring
	hashPrefix string
	hashSuffix string

	devices   map[int][]*ring.Device
	whitelist map[string]bool

	rpc  PackRpcServiceClient
	http *http.Client
}

type NodeChain struct {
	replicas int
	primary  []*ring.Device
	begin    int
	handoffs ring.MoreNodes
}

func (c *NodeChain) Next() *ring.Device {
	if c.begin < len(c.primary) {
		next := c.primary[c.begin]
		c.begin++
		return next
	}

	if c.handoffs != nil {
		return c.handoffs.Next()
	}

	return nil
}

func (r *Replicator) parseConf(cnf conf.Config) {
	r.srvPort = int(cnf.GetInt("app:object-server", "bind_port", 6000))
	r.driveRoot = cnf.GetDefault("app:object-server", "devices", "/srv/node")

	r.rpcPort = int(cnf.GetInt("object-replicator", "rpc_port", 60000))
	r.concurrency = int(cnf.GetInt("object-replicator", "concurrency", 1))
	r.interval = int(cnf.GetInt("object-replicator", "interval", 60*60*24))
}

func (r *Replicator) collectDevices(policyFilter, deviceFilter string) {
	pf := map[int]bool{}
	for _, p := range strings.Split(policyFilter, ",") {
		if p == "" {
			continue
		}

		pi, err := strconv.Atoi(p)
		if err != nil {
			r.logger.Error("unable to parse policy filter, ignore",
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

	r.rings = map[int]ring.Ring{}
	for _, p := range conf.LoadPolicies() {
		if p.Type != NAME || (len(pf) > 0 && !pf[p.Index]) {
			continue
		}

		var err error
		r.rings[p.Index], err = ring.GetRing(
			"object", r.hashPrefix, r.hashSuffix, p.Index)
		if err != nil {
			r.logger.Error("unable to get ring",
				zap.Int("policy", p.Index),
				zap.Int("port", r.srvPort),
				zap.Error(err))
			continue
		}

		r.devices = map[int][]*ring.Device{}
		devs, err := r.rings[p.Index].LocalDevices(r.srvPort)
		if err != nil {
			r.logger.Error("unable to list local device",
				zap.Int("policy", p.Index),
				zap.Int("port", r.srvPort),
				zap.Error(err))
			continue
		}

		for _, d := range devs {
			if len(df) == 0 || df[d.Device] {
				r.devices[p.Index] = append(r.devices[p.Index], d)
			}
		}

		devices := r.devices[p.Index]
		rand.Shuffle(len(devices), func(i, j int) {
			devices[i], devices[j] = devices[j], devices[i]
		})
	}
}

func (r *Replicator) listPartitions(policy int, device string) []string {
	objPath, _ := PackDevicePaths(device, r.driveRoot, policy)
	suffixes, err := fs.ReadDirNames(objPath)
	if err != nil {
		r.logger.Error("unable to get partition list", zap.Error(err))
		return nil
	}

	var partitions []string
	for _, suff := range suffixes {
		if (len(r.whitelist) > 0 && !r.whitelist[suff]) || !common.IsDecimal(suff) {
			continue
		}

		partitions = append(partitions, suff)
	}

	rand.Shuffle(len(partitions), func(i, j int) {
		partitions[i], partitions[j] = partitions[j], partitions[i]
	})

	return partitions
}

func (r *Replicator) getLocalHash(
	policy int, device, partition string, rehash []string) (int64, map[string]string) {
	// TODO: shall we need to add a timeout?
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := &SuffixHashesMsg{
		Device:      device,
		Policy:      uint32(policy),
		Partition:   partition,
		ReclaimAge:  ONE_WEEK,
		ListDir:     rand.Intn(10) == 0,
		Recalculate: rehash,
	}
	reply, err := r.rpc.GetHashes(ctx, msg)
	if err != nil {
		r.logger.Error("unable to get local hashes",
			zap.Int("policy", policy),
			zap.String("device", device),
			zap.String("partition", partition),
			zap.Error(err))
		return 0, nil
	}

	return reply.Hashed, reply.Hashes
}

func (r *Replicator) getRemoteHash(policy int, node *ring.Device,
	partition string, suffixes []string) (map[string]string, error) {
	url := fmt.Sprintf("http://%s:%d/%s/%s",
		node.Ip, node.Port, node.Device, partition)

	if len(suffixes) > 0 {
		url = fmt.Sprintf("%s/%s", url, strings.Join(suffixes, "-"))
	}

	req, err := http.NewRequest(common.REPLICATE, url, nil)
	if err != nil {
		r.logger.Error("unable to create diff request",
			zap.String("url", url),
			zap.Error(err))
		return nil, err
	}
	req.Header.Set(common.XBackendPolicyIndex, strconv.Itoa(policy))

	resp, err := r.http.Do(req)
	if err != nil {
		r.logger.Error("unable to get remote hash",
			zap.String("url", url), zap.Error(err))
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusInsufficientStorage {
		return nil, ErrRemoteDiskUnmounted
	}

	if resp.StatusCode != http.StatusOK {
		return nil, ErrRemoteHash
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		r.logger.Error("unable to read  replicate response body",
			zap.String("url", url), zap.Error(err))

		return nil, err
	}

	v, err := pickle.PickleLoads(body)
	if err != nil {
		r.logger.Error("unable to deserialize pickle data",
			zap.String("url", url), zap.Error(err))
		return nil, err
	}

	pickledHashes, ok := v.(map[interface{}]interface{})
	if !ok {
		return nil, ErrMalformedData
	}

	hashes := make(map[string]string)
	for suff, hash := range pickledHashes {
		if hashes[suff.(string)], ok = hash.(string); !ok {
			hashes[suff.(string)] = ""
		}
	}

	return hashes, nil
}

func (r *Replicator) replicateLocal(
	policy int, device *ring.Device, partition string, nodes *NodeChain) {
	rehashed, localHash := r.getLocalHash(policy, device.Device, partition, nil)
	r.stat.rehashed += rehashed

	attempts := int(r.rings[policy].ReplicaCount()) - 1
	for node := nodes.Next(); node != nil && attempts > 0; node = nodes.Next() {
		attempts--

		remoteHash, err := r.getRemoteHash(policy, node, partition, nil)
		if err != nil {
			if err == ErrRemoteDiskUnmounted {
				attempts++
			}

			continue
		}

		var suffixes []string
		for s, h := range localHash {
			if remoteHash[s] != h {
				suffixes = append(suffixes, s)
			}
		}

		if len(suffixes) == 0 {
			continue
		}
		rehashed, localHash := r.getLocalHash(
			policy, device.Device, partition, suffixes)
		r.stat.rehashed += rehashed

		suffixes = nil
		for s, h := range localHash {
			if remoteHash[s] != h {
				suffixes = append(suffixes, s)
			}
		}

		msg := &SyncMsg{
			LocalDevice: device.Device,
			Host:        node.Ip,
			Port:        int32(node.Port),
			Device:      node.Device,
			Policy:      uint32(policy),
			Partition:   partition,
			Suffixes:    suffixes,
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reply, err := r.rpc.Sync(ctx, msg)
		if err != nil {
			r.logger.Error("unable to finish sync job",
				zap.Any("args", msg), zap.Error(err))
			continue
		}

		r.getRemoteHash(policy, node, partition, suffixes)

		if reply.Success {
			r.stat.replicated += int64(len(reply.Candidates))
		}
	}
}

func (r *Replicator) replicateHandoff(
	policy int, device *ring.Device, partition string, nodes *NodeChain) {
	rehashed, localHash := r.getLocalHash(policy, device.Device, partition, nil)
	r.stat.rehashed += rehashed

	success := true
	for node := nodes.Next(); node != nil; node = nodes.Next() {
		remoteHash, err := r.getRemoteHash(policy, node, partition, nil)
		if err != nil {
			r.logger.Error("unable to get remote hash",
				zap.Int("policy", policy),
				zap.Any("node", node),
				zap.Error(err))
			success = false
			continue
		}

		var suffixes []string
		for s, h := range localHash {
			if remoteHash[s] != h {
				suffixes = append(suffixes, s)
			}
		}

		if len(suffixes) == 0 {
			continue
		}

		rehashed, localHash := r.getLocalHash(
			policy, device.Device, partition, suffixes)
		r.stat.rehashed += rehashed

		suffixes = nil
		for s, h := range localHash {
			if remoteHash[s] != h {
				suffixes = append(suffixes, s)
			}
		}

		msg := &SyncMsg{
			LocalDevice: device.Device,
			Host:        node.Ip,
			Port:        int32(node.Port),
			Device:      node.Device,
			Policy:      uint32(policy),
			Partition:   partition,
			Suffixes:    suffixes,
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reply, err := r.rpc.Sync(ctx, msg)
		if err != nil {
			r.logger.Error("unable to finish sync job",
				zap.Any("args", msg), zap.Error(err))
			success = false
			continue
		}

		if reply.Success {
			r.getRemoteHash(policy, node, partition, suffixes)
			r.stat.replicated += int64(len(reply.Candidates))
		} else {
			success = false
		}
	}

	if success {
		arg := &Partition{
			Policy:    uint32(policy),
			Device:    device.Device,
			Partition: partition,
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		r.logger.Info("removing handoff partition",
			zap.Int("policy", policy),
			zap.String("device", device.Device),
			zap.String("partition", partition))

		reply, err := r.rpc.DeleteHandoff(ctx, arg)
		if err != nil || !reply.Success {
			r.logger.Info("unable to remove handoff partition",
				zap.Int("policy", policy),
				zap.String("device", device.Device),
				zap.String("partition", partition),
				zap.Error(err))
			return
		}

		r.logger.Info("handoff partition removed",
			zap.Int("policy", policy),
			zap.String("device", device.Device),
			zap.String("partition", partition))
	}
}

func (r *Replicator) replicateDevice(
	policy int, device *ring.Device, pool chan bool, wg *sync.WaitGroup) {
	defer func() {
		<-pool
		wg.Done()
	}()

	r.logger.Info("begin to replicate device",
		zap.String("device", device.Device), zap.Int("policy", policy))

	for _, p := range r.listPartitions(policy, device.Device) {
		pi, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			r.logger.Error("unable to parse partition as integer",
				zap.String("partition", p), zap.Error(err))
			continue
		}

		// GetJobNodes will exclude the host itself
		nodes, handoff := r.rings[policy].GetJobNodes(pi, device.Id)
		chain := &NodeChain{
			replicas: int(r.rings[policy].ReplicaCount()),
			primary:  nodes,
			begin:    0,
		}

		if handoff {
			r.replicateHandoff(policy, device, p, chain)
		} else {
			chain.handoffs = r.rings[policy].GetMoreNodes(pi)
			r.replicateLocal(policy, device, p, chain)
		}
	}
}

func (r *Replicator) replicate() {
	pool := make(chan bool, r.concurrency)
	wg := &sync.WaitGroup{}

	for p, devs := range r.devices {
		for _, d := range devs {
			pool <- true
			wg.Add(1)
			go r.replicateDevice(p, d, pool, wg)
		}
	}

	wg.Wait()
}

func (r *Replicator) Run() {
	r.logger.Info("running pack replicator for once")
	r.replicate()
	r.logger.Info("replicated one pass",
		zap.Int64("rehashed", r.stat.rehashed),
		zap.Int64("replicated", r.stat.replicated))
}

func (r *Replicator) RunForever() {
	r.logger.Info("running pack replicator forever")
	for {
		r.logger.Info("begin new replication pass")
		r.replicate()
		r.logger.Info("replication pass done",
			zap.Int64("rehashed", r.stat.rehashed),
			zap.Int64("replicated", r.stat.replicated))

		r.stat.reset()
		time.Sleep(time.Second * time.Duration(r.interval))
	}
}

func InitReplicator(cnf conf.Config, flags *flag.FlagSet) (srv.Daemon, error) {
	logger, err := common.GetLogger(
		flags.Lookup("l").Value.(flag.Getter).Get().(string), "pack-replicator")
	if err != nil {
		return nil, err
	}

	r := &Replicator{
		logger: logger,
		stat:   &ReplicationStat{},
	}

	r.parseConf(cnf)

	prefix, suffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, ErrHashConfNotFound
	}
	r.hashPrefix = prefix
	r.hashSuffix = suffix

	policyFilter := flags.Lookup("policies").Value.(flag.Getter).Get().(string)
	deviceFilter := flags.Lookup("devices").Value.(flag.Getter).Get().(string)
	r.collectDevices(policyFilter, deviceFilter)

	pf := flags.Lookup("partitions").Value.(flag.Getter).Get().(string)
	r.whitelist = map[string]bool{}
	for _, p := range strings.Split(pf, ",") {
		if p != "" {
			r.whitelist[p] = true
		}
	}

	conn, err := grpc.Dial(
		fmt.Sprintf("localhost:%d", r.rpcPort), grpc.WithInsecure())
	if err != nil {
		logger.Error("unable to dial to rpc server",
			zap.Int("port", r.rpcPort), zap.Error(err))
		return nil, err
	}
	r.rpc = NewPackRpcServiceClient(conn)

	r.http = &http.Client{Timeout: 5 * time.Minute}

	return r, nil
}
