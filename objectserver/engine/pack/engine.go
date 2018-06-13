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

package pack

import (
	"flag"
	"net/textproto"
	"os"
	"strings"
	"sync"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
	"github.com/iqiyi/auklet/objectserver/engine"

	"go.uber.org/zap"
)

type PackEngine struct {
	driveRoot  string
	hashPrefix string
	hashSuffix string
	policy     int
	deviceMgr  *PackDeviceMgr
	rpcServer  *PackRpcServer
	asyncWG    *sync.WaitGroup
}

func (f *PackEngine) New(vars map[string]string,
	// Following parameters are unused at the moment
	needData bool) (engine.Object, error) {

	d := f.deviceMgr.GetPackDevice(vars["device"])
	if d == nil {
		return nil, ErrPackDeviceNotFound
	}

	obj := &PackObject{
		name:      common.ObjectName(vars["account"], vars["container"], vars["obj"]),
		device:    d,
		partition: vars["partition"],
		asyncWG:   f.asyncWG,
	}

	obj.key = generateObjectKey(f.hashPrefix, f.hashSuffix, obj.name, obj.partition)

	err := d.LoadObjectMeta(obj)
	if err != nil {
		return nil, err
	}

	// We only try to migrate the object for GET/HEAD/DELETE requests
	// because the the write request will be slow down if we try to
	// migrate every not found object.
	// For DELETE request, it is possible that the `exists` field is false
	// but there is a tombstone for the object in RocksDB. In that case,
	// we don't need to migrate anything. So we check if obj.meta is nil,
	// if no, then we are pretty sure that we have found a tombstone, thus
	// no migration is required.
	if gconf.LazyMigration && !obj.exists &&
		obj.meta == nil && vars["quse-migration"] != "" {
		if obj.Migrate() {
			if err = d.LoadObjectMeta(obj); err != nil {
				return nil, err
			}
		}
	}

	if obj.exists {
		obj.metadata = make(map[string]string)

		for k, v := range obj.meta.SystemMeta {
			obj.metadata[k] = v
		}
		for k, v := range obj.meta.UserMeta {
			obj.metadata[k] = v
		}

		obj.metadata[common.XTimestamp] = obj.meta.Timestamp
		obj.metadata["name"] = obj.meta.Name
	}

	return obj, nil
}

func (f *PackEngine) Close() error {
	glogger.Info("closing Pack Engine", zap.Int("policy", f.policy))

	f.deviceMgr.Close()

	glogger.Info("pack engine closed", zap.Int("policy", f.policy))
	return nil
}

func (f *PackEngine) GetHashes(
	device, partition string, recalculate []string) (map[string]string, error) {
	dev := f.deviceMgr.GetPackDevice(device)
	if dev == nil {
		return nil, ErrPackDeviceNotFound
	}

	_, hashes, err := dev.GetHashes(partition, recalculate, false, common.ONE_WEEK)
	return hashes, err
}

func (f *PackEngine) DiffReplicas(device, partition string,
	objects map[string]*ObjectTimestamps) (map[string]*WantedParts, error) {
	dev := f.deviceMgr.GetPackDevice(device)
	if dev == nil {
		return nil, ErrPackDeviceNotFound
	}

	wanted := make(map[string]*WantedParts)
	for h, ts := range objects {
		// Ignore the error at the moment.
		// Shall we stop or ignore the error?
		if w, err := dev.DiffReplica(partition, h, ts); err == nil {
			wanted[h] = w
		}
	}

	return wanted, nil
}

func PackEngineConstructor(config conf.Config, policy *conf.Policy,
	flags *flag.FlagSet, wg *sync.WaitGroup) (engine.ObjectEngine, error) {

	var err error
	glogger, err = common.GetLogger(
		flags.Lookup("l").Value.(flag.Getter).Get().(string), "pack")
	if err != nil {
		common.BootstrapLogger.Printf("unable to config zap log: %v", err)
		os.Exit(1)
	}

	driveRoot := config.GetDefault("app:object-server", "devices", "/srv/node")
	prefix, suffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, ErrHashConfNotFound
	}
	gconf = &PackConfig{
		AuditorFPS:        config.GetInt("object-auditor", "files_per_second", 20),
		AuditorBPS:        config.GetInt("object-auditor", "bytes_per_second", 10*1024*1024),
		LazyMigration:     config.GetBool("object-pack", "lazy_migration", false),
		PackChunkedObject: config.GetBool("object-pack", "pack_chunked_object", false),
	}

	gconf.AllowedHeaders = map[string]bool{
		common.HContentDisposition: true,
		common.HContentEncoding:    true,
		common.XDeleteAt:           true,
		common.XObjectManifest:     true,
		common.XStaticLargeObject:  true,
	}
	if eah, ok := config.Get("app:object-server", "allowed_headers"); ok {
		for _, h := range strings.Split(eah, ",") {
			ch := textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(h))
			gconf.AllowedHeaders[ch] = true
		}
	}

	port := int(config.GetInt("app:object-server", "bind_port", 6000))

	dm := NewPackDeviceMgr(port, driveRoot, policy.Index)
	if dm == nil {
		panic("failed to load pack device manager")
	}
	dm.testMode = config.GetBool("object-pack", "test_mode", false)
	if !dm.testMode {
		go dm.monitorDisks()
	}

	rpcPort := int(config.GetInt("app:object-server", "rpc_port", 60000))
	rpc, ok := rpcServers[port]
	if !ok {
		rpc = NewRpcServer(rpcPort)
		// Each rpc server is identified by object port instead of rpc port
		rpcServers[port] = rpc
		go rpc.start()
	}
	rpc.RegisterPackDeviceMgr(dm)

	return &PackEngine{
		driveRoot:  driveRoot,
		policy:     policy.Index,
		hashPrefix: prefix,
		hashSuffix: suffix,
		deviceMgr:  dm,
		asyncWG:    wg,
	}, nil
}

func init() {
	engine.RegisterObjectEngine(NAME, PackEngineConstructor)
}
