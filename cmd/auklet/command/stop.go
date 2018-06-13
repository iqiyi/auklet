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
	"log"
	"strings"
)

type StopCommand struct {
	Logger *log.Logger
}

func (c *StopCommand) Help() string {
	helpText := `
Usage: auklet stop [server name]

  Stop a server
`
	return strings.TrimSpace(helpText)
}

func (c *StopCommand) Run(args []string) int {
	if len(args) < 1 {
		c.Logger.Println(c.Help())
		return EXIT_USAGE
	}
	err := stopServer(args[0])
	if err != nil {
		c.Logger.Printf("unable to stop %s server: %v", args[0], err)
		return EXIT_STOP
	}

	c.Logger.Printf("%s server stopped", args[0])
	return EXIT_OK
}

func (c *StopCommand) Synopsis() string {
	return "stop a server"

}
