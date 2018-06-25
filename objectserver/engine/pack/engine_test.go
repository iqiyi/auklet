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
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
)

func TestInitPackEngine(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	confTemplate := `
[app:object-server]
devices=%s
mount_check=false
bind_port=%d
rpc_port=%d
test_mode = yes
	`
	configString := fmt.Sprintf(confTemplate, root,
		common.RandIntInRange(40000, 50000), common.RandIntInRange(50001, 60000))

	cnf, err := conf.StringConfig(configString)
	require.Nil(t, err)

	flags := flag.NewFlagSet("pack engine ut", flag.ExitOnError)
	flags.String("l", "", "zap yaml log config file")

	policy := &conf.Policy{
		Index:   0,
		Type:    "pack",
		Name:    "Policy-0",
		Default: true,
	}

	eng, err := PackEngineConstructor(cnf, policy, flags, &sync.WaitGroup{})
	require.Nil(t, err)
	require.NotNil(t, eng)
}

func TestPackEngineNewObject1(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	confTemplate := `
[app:object-server]
test_mode = yes
devices=%s
mount_check=false
bind_port=%d
rpc_port=%d
	`
	configString := fmt.Sprintf(confTemplate, root,
		common.RandIntInRange(40000, 50000), common.RandIntInRange(50001, 60000))

	cnf, err := conf.StringConfig(configString)
	require.Nil(t, err)

	flags := flag.NewFlagSet("pack engine ut", flag.ExitOnError)
	flags.String("l", "", "zap yaml log config file")

	policy := &conf.Policy{
		Index:   0,
		Type:    "pack",
		Name:    "Policy-0",
		Default: true,
	}

	eng, _ := PackEngineConstructor(cnf, policy, flags, &sync.WaitGroup{})

	vars := map[string]string{
		"account":   "a",
		"container": "c",
		"object":    fmt.Sprintf("o-%010d", rand.Int31()),
		"partition": strconv.Itoa(int(rand.Int31())),
	}

	obj, err := eng.New(vars, false)
	require.Nil(t, err)
	require.NotNil(t, obj)
	require.False(t, obj.Exists())
}

func TestPackEngineNewObject2(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	confTemplate := `
[app:object-server]
devices=%s
mount_check=false
bind_port=%d
rpc_port=%d
test_mode = yes
	`
	configString := fmt.Sprintf(confTemplate, root,
		common.RandIntInRange(40000, 50000), common.RandIntInRange(50001, 60000))

	cnf, _ := conf.StringConfig(configString)

	flags := flag.NewFlagSet("pack engine ut", flag.ExitOnError)
	flags.String("l", "", "zap yaml log config file")

	policy := &conf.Policy{
		Index:   0,
		Type:    "pack",
		Name:    "Policy-0",
		Default: true,
	}

	eng, _ := PackEngineConstructor(cnf, policy, flags, &sync.WaitGroup{})

	vars := map[string]string{
		"account":   "a",
		"container": "c",
		"obj":       fmt.Sprintf("o-%010d", rand.Int31()),
		"partition": strconv.Itoa(int(rand.Int31())),
		"device":    PACK_DEVICE,
	}

	obj, err := eng.New(vars, false)
	require.Nil(t, err)

	size := rand.Int63n(SIZE_1M * 8)
	w, _ := obj.SetData(size)
	data := generateData(size)
	w.Write(data)
	metadata := map[string]string{
		"name":                fmt.Sprintf("/%s/%s/%s", vars["account"], vars["container"], vars["obj"]),
		common.XTimestamp:     common.GetTimestamp(),
		common.HContentType:   common.VOctetStream,
		common.HContentLength: strconv.FormatInt(size, 10),
		common.HEtag:          bytesMd5(data),
	}
	require.Nil(t, obj.Commit(metadata))
	require.Nil(t, obj.Close())

	require.Nil(t, eng.(*PackEngine).Close())

	eng, _ = PackEngineConstructor(cnf, policy, flags, &sync.WaitGroup{})
	obj2, err := eng.New(vars, false)
	require.Nil(t, err)
	require.True(t, obj2.Exists())
}
