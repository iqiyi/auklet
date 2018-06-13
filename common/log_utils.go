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

package common

import (
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/hashicorp/go-syslog"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

// Log to stdout and rsyslog used during bootstrap.
var BootstrapLogger *log.Logger

func init() {
	var sink io.Writer
	syslog, err := gsyslog.NewLogger(gsyslog.LOG_NOTICE, "LOCAL0", "auklet")
	if err != nil {
		sink = io.MultiWriter(os.Stdout)
	} else {
		sink = io.MultiWriter(os.Stdout, syslog)
	}

	BootstrapLogger = log.New(sink, "", log.Flags()&^log.LstdFlags)
}

func GetLogger(conf, name string) (*zap.Logger, error) {
	if conf == "" {
		BootstrapLogger.Println(
			"no config file path provided, return default logger which logs to /tmp/auklet.log and stderr")
		c := zap.NewProductionConfig()
		c.InitialFields = map[string]interface{}{"name": name}
		c.OutputPaths = append(c.OutputPaths, "/tmp/auklet.log")
		return c.Build()
	}

	raw, err := ioutil.ReadFile(conf)
	if err != nil {
		BootstrapLogger.Printf("unable to read config file: %v", err)
		return nil, err
	}

	sections := make(map[string]*zap.Config)
	if err = yaml.Unmarshal(raw, sections); err != nil {
		BootstrapLogger.Printf(
			"unable to parse config file, only yaml is supported: %v", err)
		return nil, err
	}

	c := sections[name]
	if c == nil {
		BootstrapLogger.Printf("log config section for %s is not found", name)
		return nil, ErrLogConfigNotFound
	}

	return c.Build()
}
