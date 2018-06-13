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
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	context "golang.org/x/net/context"

	"github.com/iqiyi/auklet/common/fs"
)

func TestRpcGetHashes(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	mgr := NewPackDeviceMgr(6000, root, PACK_POLICY_INDEX)
	mgr.testMode = true

	rpc := NewRpcServer(60000)
	rpc.RegisterPackDeviceMgr(mgr)
	d, _ := rpc.getDevice(PACK_POLICY_INDEX, PACK_DEVICE)

	size := rand.Int63n(NEEDLE_THRESHOLD * 2)
	obj := newPackObject(size, "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	obj.Close()

	vo := copyVanilla(obj)
	d.LoadObjectMeta(vo)
	vo.meta.UserMeta["X-Object-Meta-Tag"] = "dev"
	d.CommitUpdate(vo)
	vo.Close()

	str := obj.meta.Timestamp + vo.meta.Timestamp
	expected := map[string]string{
		splitObjectKey(obj.key)[1]: bytesMd5([]byte(str)),
	}

	msg := &SuffixHashesMsg{
		Device:     PACK_DEVICE,
		Policy:     PACK_POLICY_INDEX,
		Partition:  obj.partition,
		ReclaimAge: ONE_WEEK,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reply, err := rpc.GetHashes(ctx, msg)
	require.Nil(t, err)

	require.Equal(t, int64(1), reply.Hashed)
	require.Equal(t, expected, reply.Hashes)
}

func TestRpcDeleteHandoff(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	mgr := NewPackDeviceMgr(6000, root, PACK_POLICY_INDEX)
	mgr.testMode = true

	rpc := NewRpcServer(60000)
	require.NotNil(t, rpc)
	rpc.RegisterPackDeviceMgr(mgr)
	d, err := rpc.getDevice(PACK_POLICY_INDEX, PACK_DEVICE)
	require.Nil(t, err)

	size := rand.Int63n(NEEDLE_THRESHOLD * 2)
	obj := newPackObject(size, "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	dir := filepath.Join(d.objectsDir, obj.key)
	InvalidateHash(dir)
	obj.Close()

	msg := &Partition{
		Device:    PACK_DEVICE,
		Policy:    PACK_POLICY_INDEX,
		Partition: obj.partition,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pd, _, _ := d.hashesPaths(obj.partition)
	require.False(t, fs.IsFileNotExist(pd))
	reply, err := rpc.DeleteHandoff(ctx, msg)
	require.Nil(t, err)
	require.True(t, reply.Success)
	require.True(t, fs.IsFileNotExist(pd))
}
