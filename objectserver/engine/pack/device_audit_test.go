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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iqiyi/auklet/common"
)

func TestAuditPartition1(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	size := rand.Int63n(NEEDLE_THRESHOLD * 2)
	obj := newPackObject(size, "")
	feedObject(obj, d)
	d.CommitWrite(obj)

	stat, err := d.AuditPartition(obj.partition)
	require.Nil(t, err)
	require.Equal(t, size, stat.ProcessedBytes)
	require.Equal(t, int64(0), stat.Quarantines)
}

func TestAuditPartition2(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	so := newPackSO("")
	lo := newPackLO(so.partition)
	feedObject(so, d)
	feedObject(lo, d)
	d.CommitWrite(so)
	d.CommitWrite(lo)

	stat, err := d.AuditPartition(so.partition)
	require.Nil(t, err)
	require.Equal(t, so.dataSize+lo.dataSize, stat.ProcessedBytes)
	require.Equal(t, int64(0), stat.Quarantines)
}

func TestAuditBadObject1(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	size := rand.Int63n(NEEDLE_THRESHOLD * 2)
	obj := newPackObject(size, "")
	feedObject(obj, d)
	obj.meta.SystemMeta[common.HEtag] = "abcdefghijklmnopqrstuvwxyzabcdef"
	d.CommitWrite(obj)

	stat, err := d.AuditPartition(obj.partition)
	require.Nil(t, err)
	require.Equal(t, int64(1), stat.Quarantines)
}

func TestAuditBadObject2(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	so := newPackSO("")
	lo := newPackLO(so.partition)
	feedObject(so, d)
	feedObject(lo, d)
	so.meta.SystemMeta[common.HEtag] = "abcdefghijklmnopqrstuvwxyzabcdef"
	lo.meta.SystemMeta[common.HEtag] = "defabcdefghijklmnopqrstuvwxyzabc"
	d.CommitWrite(so)
	d.CommitWrite(lo)

	stat, err := d.AuditPartition(so.partition)
	require.Nil(t, err)
	require.Equal(t, so.dataSize+lo.dataSize, stat.ProcessedBytes)
	require.Equal(t, int64(2), stat.Quarantines)

	v1 := copyVanilla(so)
	v2 := copyVanilla(lo)

	d.LoadObjectMeta(v1)
	d.LoadObjectMeta(v2)

	require.False(t, v1.exists)
	require.False(t, v2.exists)
}
