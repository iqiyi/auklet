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
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/mitchellh/cli"
	rocksdb "github.com/tecbot/gorocksdb"

	"github.com/iqiyi/auklet/objectserver/engine/pack"
)

type DumpDBCommand struct {
	Ui cli.Ui
}

func (c *DumpDBCommand) Help() string {
	helpText := `
Usage: auklet dump-db -d db -p prefix
`
	return strings.TrimSpace(helpText)
}

func (c *DumpDBCommand) Run(args []string) int {
	var dbPath, prefix string
	flags := flag.NewFlagSet("dump-db", flag.ExitOnError)
	flags.StringVar(&dbPath, "d", "", "path of RocksDB")
	flags.StringVar(&prefix, "p", "", "prefix of db keys")
	if err := flags.Parse(args); err != nil {
		return EXIT_USAGE
	}

	if dbPath == "" || prefix == "" {
		c.Ui.Output(c.Help())
		return EXIT_USAGE
	}

	opts := rocksdb.NewDefaultOptions()
	db, err := rocksdb.OpenDbForReadOnly(opts, dbPath, true)
	if err != nil {
		fmt.Printf("unable to open RocksDB, %v\n", err)
		return EXIT_ERROR
	}

	ropt := rocksdb.NewDefaultReadOptions()
	iter := db.NewIterator(ropt)
	defer iter.Close()
	pre := []byte(prefix)
	for iter.Seek(pre); iter.ValidForPrefix(pre); iter.Next() {
		b := iter.Value().Data()
		index := new(pack.DBIndex)
		if err := proto.Unmarshal(b, index); err != nil {
			panic(err)
		}

		if b, err = json.Marshal(index); err != nil {
			panic(err)
		}

		fmt.Println(string(b))
	}

	return EXIT_OK
}

func (c *DumpDBCommand) Synopsis() string {
	return "dump the index from RocksDB"
}
