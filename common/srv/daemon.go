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
//

package srv

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
)

type Daemon interface {
	Run()
	RunForever()
}

type DaemonConstructor func(conf.Config, *flag.FlagSet) (Daemon, error)

func RunDaemon(initDaemon DaemonConstructor, flags *flag.FlagSet) error {
	var daemons []Daemon
	logger := common.BootstrapLogger

	configFile := flags.Lookup("c").Value.(flag.Getter).Get().(string)
	configs, err := conf.LoadConfigs(configFile)
	if err != nil {
		logger.Printf("unable to load daemon config: %v", err)
		return err
	}

	once := flags.Lookup("once").Value.(flag.Getter).Get() == true

	for _, config := range configs {
		daemon, err := initDaemon(config, flags)
		if err != nil {
			logger.Printf("unable to initialize daemon: %v", err)
			return err
		}

		if once {
			daemon.Run()
			logger.Println("daemon pass completed")
		} else {
			daemons = append(daemons, daemon)
			go daemon.RunForever()
			logger.Println("daemon started")
		}
	}

	if len(daemons) > 0 {
		c := make(chan os.Signal, 1)
		signal.Notify(c,
			syscall.SIGHUP,
			syscall.SIGTERM,
			syscall.SIGINT,
			syscall.SIGQUIT,
			syscall.SIGABRT)
		switch <-c {
		case syscall.SIGABRT, syscall.SIGQUIT:
			pid := os.Getpid()
			DumpGoroutinesStackTrace(pid)
		}
	}

	return nil
}
