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

package command

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/iqiyi/auklet/common/conf"
	"github.com/iqiyi/auklet/common/srv"
	"github.com/iqiyi/auklet/objectserver/engine/pack"
)

type PackAuditorCommand struct {
	Logger *log.Logger
}

func (c *PackAuditorCommand) Help() string {
	helpText := `
Usage: auklet pack-auditor [-c config] [-once]

  Start auditor of pack engine
`
	return strings.TrimSpace(helpText)
}

func (c *PackAuditorCommand) Run(args []string) int {
	defer func() {
		if err := recover(); err != nil {
			c.Logger.Printf("%v", err)
		}
	}()

	flags := flag.NewFlagSet("pack auditor", flag.ExitOnError)
	flags.Usage = func() { fmt.Println(c.Help()) }
	flags.String("c", conf.FindServerConfig("object"), "config file/directory")
	flags.String("l", "", "zap yaml log config file")
	flags.Bool("once", false, "run one pass of the auditor")
	flags.String("policies", "", "policy filter")
	flags.String("devices", "", "device filter")
	flags.String("partitions", "", "partition filter")
	if err := flags.Parse(args); err != nil {
		return EXIT_USAGE
	}

	if flags.NArg() > 0 {
		c.Logger.Println(c.Help())
		return EXIT_USAGE
	}

	if err := srv.RunDaemon(pack.InitAuditor, flags); err != nil {
		c.Logger.Printf("unable to run pack auditor: %v", err)
		return EXIT_START
	}

	return EXIT_OK
}

func (c *PackAuditorCommand) Synopsis() string {
	return "start pack auditor"
}
