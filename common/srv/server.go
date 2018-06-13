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

package srv

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
)

type Server interface {
	Start() error
	Shutdown(ctx context.Context) error
	Close() error
	Finalize()
}

type ServerConstructor func(conf.Config, *flag.FlagSet) (Server, error)

func RunServers(initServer ServerConstructor, flags *flag.FlagSet) error {
	var servers []Server
	logger := common.BootstrapLogger

	configFile := flags.Lookup("c").Value.(flag.Getter).Get().(string)
	configs, err := conf.LoadConfigs(configFile)
	if err != nil {
		return err
	}

	for _, config := range configs {
		server, err := initServer(config, flags)
		if err != nil {
			return err
		}

		go server.Start()
		servers = append(servers, server)
	}

	if len(servers) == 0 {
		return ErrNoServerFound
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c,
		syscall.SIGHUP,
		syscall.SIGTERM,
		syscall.SIGINT,
		syscall.SIGQUIT,
		syscall.SIGABRT)

	s := <-c
	switch s {
	case syscall.SIGTERM, syscall.SIGHUP: // graceful shutdown
		var wg sync.WaitGroup
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
		defer cancel()
		for _, srv := range servers {
			wg.Add(1)
			go func(hserv Server) {
				defer wg.Done()
				hserv.Shutdown(ctx)
				hserv.Finalize()
			}(srv)
		}
		wgc := make(chan struct{})
		go func() {
			defer close(wgc)
			wg.Wait()
		}()

		select {
		case <-wgc:
			logger.Println("graceful shutdown complete")
			return nil
		case <-ctx.Done():
			logger.Println("forcing shutdown after timeout.")
			return nil
		}
	case syscall.SIGABRT, syscall.SIGQUIT:
		pid := os.Getpid()
		DumpGoroutinesStackTrace(pid)
	default: // force close
		for _, srv := range servers {
			if err := srv.Close(); err != nil {
				logger.Println("forcing closing")
			}
		}
	}

	return nil
}
