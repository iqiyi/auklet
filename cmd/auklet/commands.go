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

package main

import (
	"os"

	"github.com/mitchellh/cli"

	"github.com/iqiyi/auklet/cmd/auklet/command"
	"github.com/iqiyi/auklet/common"
)

var commands map[string]cli.CommandFactory

func init() {
	logger := common.BootstrapLogger
	ui := &cli.BasicUi{Writer: os.Stdout}

	commands = map[string]cli.CommandFactory{
		"version": func() (cli.Command, error) {
			return &command.VersionCommand{}, nil
		},

		"start": func() (cli.Command, error) {
			return &command.StartCommand{
				Logger: logger,
			}, nil
		},

		"stop": func() (cli.Command, error) {
			return &command.StopCommand{
				Logger: logger,
			}, nil
		},

		"shutdown": func() (cli.Command, error) {
			return &command.ShutdownCommand{
				Logger: logger,
			}, nil
		},

		"restart": func() (cli.Command, error) {
			return &command.RestartCommand{
				Logger: logger,
			}, nil
		},

		"reload": func() (cli.Command, error) {
			return &command.ReloadCommand{
				Logger: logger,
			}, nil
		},

		"object": func() (cli.Command, error) {
			return &command.ObjectCommand{
				Logger: logger,
			}, nil
		},

		"pack-auditor": func() (cli.Command, error) {
			return &command.PackAuditorCommand{
				Logger: logger,
			}, nil
		},

		"pack-replicator": func() (cli.Command, error) {
			return &command.PackReplicatorCommand{
				Logger: logger,
			}, nil
		},

		"dump-db": func() (cli.Command, error) {
			return &command.DumpDBCommand{
				Ui: ui,
			}, nil
		},
	}
}
