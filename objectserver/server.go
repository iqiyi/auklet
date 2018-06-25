// Copyright (c) 2015 Rackspace
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

package objectserver

import (
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/textproto"
	"strings"
	"sync"
	"time"

	"github.com/justinas/alice"
	"github.com/uber-go/tally"
	promreporter "github.com/uber-go/tally/prometheus"
	"go.uber.org/zap"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
	"github.com/iqiyi/auklet/common/fs"
	"github.com/iqiyi/auklet/common/middleware"
	"github.com/iqiyi/auklet/common/srv"
	"github.com/iqiyi/auklet/objectserver/engine"

	// Register different engine
	_ "github.com/iqiyi/auklet/objectserver/engine/pack"
	_ "github.com/iqiyi/auklet/objectserver/engine/swift"
)

type ObjectServer struct {
	http.Server

	driveRoot        string
	checkEtags       bool
	checkMounts      bool
	logger           *zap.Logger
	diskInUse        *common.KeyedLimit
	accountDiskInUse *common.KeyedLimit
	expiringDivisor  int64
	updateClient     *http.Client
	objEngines       map[int]engine.ObjectEngine
	updateTimeout    time.Duration
	asyncWG          *sync.WaitGroup // Used to wait on async goroutines

	metricsCloser io.Closer

	ip         string
	port       int
	hashPrefix string
	hashSuffix string

	// header filters
	blacklist map[string]bool
	whitelist map[string]bool

	asyncJobMgr AsyncJobMgr
}

func (s *ObjectServer) Finalize() {
	// This method will not be called until http server is shut down
	// gracefully which means all the pending http requests will be served.
	// So we can close engines almost safely. Hoooooray!
	// I said 'almost' because there are possible races with
	// auditor and replicator. But it is ok because we just need to guarantee
	// the consistency of the rocksdb and volume files in pack engine.
	for index, engine := range s.objEngines {
		if err := engine.Close(); err != nil {
			s.logger.Error("unable to close object engine",
				zap.Int("policy", index), zap.Error(err))
		}
	}

	if s.metricsCloser != nil {
		s.metricsCloser.Close()
	}

	s.asyncWG.Wait()
}

func (s *ObjectServer) buildHandler(config conf.Config) http.Handler {
	reporter := promreporter.NewReporter(promreporter.Options{})
	var metricsScope tally.Scope
	metricsScope, s.metricsCloser = tally.NewRootScope(
		tally.ScopeOptions{
			Prefix:         fmt.Sprintf("auklet_object_%d", s.port),
			Tags:           map[string]string{},
			CachedReporter: reporter,
			Separator:      promreporter.DefaultSeparator,
		}, time.Second)

	wares := alice.New(
		s.RequestLogger,
		middleware.RecoverHandler,
		middleware.RequestValidator,
		s.DeviceAcquirer,
	)
	router := srv.NewRouter()
	router.Get("/metrics", reporter.HTTPHandler())
	router.Get("/healthcheck", wares.ThenFunc(s.HealthcheckHandler))
	router.Get("/diskusage", wares.ThenFunc(s.DiskUsageHandler))
	router.Get("/recon/:method/:recon_type", wares.ThenFunc(s.ReconHandler))
	router.Get("/recon/:method", wares.ThenFunc(s.ReconHandler))
	router.Get("/debug/pprof/:parm", http.DefaultServeMux)
	router.Post("/debug/pprof/:parm", http.DefaultServeMux)

	router.Get("/:device/:partition/:account/:container/*obj",
		wares.ThenFunc(s.ObjGetHandler))
	router.Head("/:device/:partition/:account/:container/*obj",
		wares.ThenFunc(s.ObjGetHandler))
	router.Put("/:device/:partition/:account/:container/*obj",
		wares.ThenFunc(s.ObjPutHandler))
	router.Post("/:device/:partition/:account/:container/*obj",
		wares.ThenFunc(s.ObjPostHandler))
	router.Delete("/:device/:partition/:account/:container/*obj",
		wares.ThenFunc(s.ObjDeleteHandler))
	router.Options("/", wares.ThenFunc(s.OptionsHandler))

	for _, p := range conf.LoadPolicies() {
		router.HandlePolicy("REPLICATE", "/:device/:partition/:suffixes",
			p.Index, http.HandlerFunc(s.ReplicateHandler))
		router.HandlePolicy("REPLICATE", "/:device/:partition",
			p.Index, http.HandlerFunc(s.ReplicateHandler))
		router.HandlePolicy("DIFF", "/:device/:partition",
			p.Index, http.HandlerFunc(s.DiffReplicasHandler))
	}

	router.NotFoundHandler = http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			msg := fmt.Sprintf("Invalid path: %s", r.URL.Path)
			common.CustomResponse(w, http.StatusBadRequest, msg)
		})

	return alice.New(middleware.RequestMetrics(metricsScope)).Then(router)
}

func (s *ObjectServer) isHeaderAllowed(header string) bool {
	if s.blacklist[header] {
		return false
	}

	return common.IsUserMeta("object", header) ||
		common.IsSysMeta("object", header) ||
		s.whitelist[header]
}

func (s *ObjectServer) Start() error {
	sock, err := srv.RetryListen(s.ip, s.port)
	if err != nil {
		s.logger.Error("error listening", zap.Error(err))
		return err
	}
	return s.Serve(sock)
}

func startKVRpcService(cnf conf.Config, flags *flag.FlagSet) {
	driveRoot := cnf.GetDefault("app:object-server", "devices", "/srv/node")

	ringPort := int(cnf.GetInt("DEFAULT", "bind_port", 6000))
	kv := NewKVStore(driveRoot, ringPort)
	test := cnf.GetBool("app:object-server", "test_mode", false)
	if !test {
		m := fs.NewMountMonitor()
		m.RegisterCallback("async-job-mgr", kv.mountListener)
		go m.Start()
	} else {
		kv.setTestMode(true)
	}

	var rpcSvc *KVService
	rpcPort := int(cnf.GetInt("app:object-server", "async_kv_service_port", 60001))
	if cnf.GetBool("app:object-server", "async_kv_fs_compatible", false) {
		fs := NewFSStore(driveRoot)
		rpcSvc = NewKVFSService(fs, kv, rpcPort)
	} else {
		rpcSvc = NewKVService(kv, rpcPort)
	}

	go rpcSvc.start()
}

func (s *ObjectServer) initAsyncJobMgr(
	cnf conf.Config, flags *flag.FlagSet) error {
	var err error
	mgr := cnf.GetDefault("app:object-server", "async_job_manager", "fs")
	if mgr == "kv" {
		startKVRpcService(cnf, flags)
	}

	s.asyncJobMgr, err = NewAsyncJobMgr(cnf, flags)
	if err != nil {
		common.BootstrapLogger.Printf("unable to initialize kv async job mgr: %v", err)
	}
	return err
}

func InitServer(config conf.Config, flags *flag.FlagSet) (
	srv.Server, error) {
	prefix, suffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, ErrHashConfNotFound
	}

	server := &ObjectServer{
		driveRoot:  "/srv/node",
		asyncWG:    &sync.WaitGroup{},
		hashPrefix: prefix,
		hashSuffix: suffix,
	}

	server.logger, err = common.GetLogger(
		flags.Lookup("l").Value.(flag.Getter).Get().(string), "object-server")
	if err != nil {
		return nil, err
	}
	glogger = server.logger

	server.objEngines = make(map[int]engine.ObjectEngine)
	for _, p := range conf.LoadPolicies() {
		newEngine, err := engine.FindEngine(p.Type)
		if err != nil {
			server.logger.Error("object engine not found",

				zap.String("engine", p.Type), zap.Error(err))
			return nil, err
		}

		server.objEngines[p.Index], err = newEngine(config, p, flags, server.asyncWG)
		if err != nil {
			server.logger.Error("unable to initialize object engine",
				zap.String("engine", p.Type), zap.Error(err))
			return nil, err
		}
	}

	server.driveRoot = config.GetDefault(
		"app:object-server", "devices", "/srv/node")
	server.checkMounts = config.GetBool("app:object-server", "mount_check", true)
	server.checkEtags = config.GetBool("app:object-server", "check_etags", false)
	server.diskInUse = common.NewKeyedLimit(
		config.GetLimit("app:object-server", "disk_limit", 25, 0))
	server.accountDiskInUse = common.NewKeyedLimit(
		config.GetLimit("app:object-server", "account_rate_limit", 20, 0))
	server.expiringDivisor = config.GetInt(
		"app:object-server", "expiring_objects_container_divisor", 86400)

	server.blacklist = map[string]bool{
		common.HContentType:   true,
		common.HContentLength: true,
		common.HEtag:          true,
		"Deleted":             true,
	}
	server.whitelist = map[string]bool{
		common.HContentDisposition: true,
		common.HContentEncoding:    true,
		common.XDeleteAt:           true,
		common.XObjectManifest:     true,
		common.XStaticLargeObject:  true,
	}
	if ah, ok := config.Get("app:object-server", "allowed_headers"); ok {
		for _, h := range strings.Split(ah, ",") {
			header := textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(h))
			if !server.blacklist[header] {
				server.whitelist[header] = true
			}
		}
	}

	timeout := config.GetFloat(
		"app:object-server", "container_update_timeout", 0.25)
	server.updateTimeout = time.Duration(timeout * float64(time.Second))
	timeout = config.GetFloat("app:object-server", "conn_timeout", 1.0)
	connTimeout := time.Duration(timeout * float64(time.Second))
	timeout = config.GetFloat("app:object-server", "node_timeout", 10.0)
	nodeTimeout := time.Duration(timeout * float64(time.Second))
	server.updateClient = &http.Client{
		Timeout: nodeTimeout,
		Transport: &http.Transport{
			Dial: (&net.Dialer{Timeout: connTimeout}).Dial,
		},
	}

	deviceLockUpdateSeconds := config.GetInt(
		"app:object-server", "device_lock_update_seconds", 0)
	if deviceLockUpdateSeconds > 0 {
		go server.updateDeviceLocks(deviceLockUpdateSeconds)
	}

	server.ip = config.GetDefault("app:object-server", "bind_ip", "0.0.0.0")
	server.port = int(config.GetInt("app:object-server", "bind_port", 6000))

	server.Server = http.Server{
		Handler:      server.buildHandler(config),
		ReadTimeout:  24 * time.Hour,
		WriteTimeout: 24 * time.Hour,
	}

	if err := server.initAsyncJobMgr(config, flags); err != nil {
		return nil, err
	}

	return server, nil
}
