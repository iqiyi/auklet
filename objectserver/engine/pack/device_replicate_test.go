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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/fs"
)

func TestIsSuffixExists(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	size := rand.Int63n(NEEDLE_THRESHOLD * 2)
	obj := newPackObject(size, "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	obj.Close()

	suffix := splitObjectKey(obj.key)[1]
	require.True(t, d.isSuffixExists(obj.partition, suffix))
}

func TestListSuffixes(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	so := newPackSO("")
	lo := newPackLO(so.partition)
	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD*2), so.partition)
	feedObject(so, d)
	feedObject(lo, d)
	feedObject(obj, d)
	d.CommitWrite(so)
	d.CommitWrite(lo)
	d.CommitWrite(obj)
	so.Close()
	lo.Close()
	obj.Close()

	expected := []string{
		splitObjectKey(so.key)[1],
		splitObjectKey(lo.key)[1],
		splitObjectKey(obj.key)[1],
	}
	actual := d.ListSuffixes(so.partition)

	sort.Strings(expected)
	sort.Strings(actual)

	require.Equal(t, expected, actual)
}

func TestGetHashes1(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	so := newPackSO("")
	lo := newPackLO(so.partition)
	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD*2), so.partition)
	feedObject(so, d)
	feedObject(lo, d)
	feedObject(obj, d)
	d.CommitWrite(so)
	d.CommitWrite(lo)
	d.CommitWrite(obj)
	so.Close()
	lo.Close()
	obj.Close()

	expected := map[string]string{
		splitObjectKey(so.key)[1]:  bytesMd5([]byte(so.meta.Timestamp)),
		splitObjectKey(lo.key)[1]:  bytesMd5([]byte(lo.meta.Timestamp)),
		splitObjectKey(obj.key)[1]: bytesMd5([]byte(obj.meta.Timestamp)),
	}
	hashed, actual, err := d.GetHashes(so.partition, nil, false, ONE_WEEK)
	require.Nil(t, err)
	require.Equal(t, int64(3), hashed)
	require.Equal(t, expected, actual)
}

func TestGetHashes2(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

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
	hashed, actual, err := d.GetHashes(obj.partition, nil, false, ONE_WEEK)
	require.Nil(t, err)
	require.Equal(t, int64(1), hashed)
	require.Equal(t, expected, actual)
}

func TestDeleteHandoff(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	size := rand.Int63n(NEEDLE_THRESHOLD * 2)
	obj := newPackObject(size, "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	dir := filepath.Join(d.objectsDir, obj.key)
	InvalidateHash(dir)
	obj.Close()

	pd, _, _ := d.hashesPaths(obj.partition)
	require.False(t, fs.IsFileNotExist(pd))
	require.Nil(t, d.DeleteHandoff(obj.partition))
	require.True(t, fs.IsFileNotExist(pd))
}

func TestListSuffixTimestamps1(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD*2), "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	obj.Close()

	expected := map[string]*ObjectTimestamps{
		splitObjectKey(obj.key)[2]: {obj.meta.Timestamp, ""},
	}
	actual, err := d.ListSuffixTimestamps(obj.partition, splitObjectKey(obj.key)[1])
	require.Nil(t, err)
	require.Equal(t, expected, actual)
}

func TestListSuffixTimestamps2(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD*2), "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	obj.Close()

	vo := copyVanilla(obj)
	d.LoadObjectMeta(vo)
	vo.meta.UserMeta["X-Object-Meta-Tag"] = "dev"
	d.CommitUpdate(vo)
	vo.Close()

	expected := map[string]*ObjectTimestamps{
		splitObjectKey(obj.key)[2]: {
			obj.meta.Timestamp,
			vo.meta.Timestamp,
		},
	}
	actual, err := d.ListSuffixTimestamps(obj.partition, splitObjectKey(obj.key)[1])
	require.Nil(t, err)
	require.Equal(t, expected, actual)
}

func TestDiffReplica1(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD*2), "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	obj.Close()

	tses := &ObjectTimestamps{obj.meta.Timestamp, ""}
	wanted, err := d.DiffReplica(obj.partition, splitObjectKey(obj.key)[2], tses)
	require.Nil(t, err)
	require.Equal(t, &WantedParts{false, false}, wanted)
}

func TestDiffReplica2(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD*2), "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	obj.Close()

	tses := &ObjectTimestamps{common.GetTimestamp(), ""}
	wanted, err := d.DiffReplica(obj.partition, splitObjectKey(obj.key)[2], tses)
	require.Nil(t, err)
	require.Equal(t, &WantedParts{true, false}, wanted)
}

func TestDiffReplica3(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD*2), "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	obj.Close()

	tses := &ObjectTimestamps{obj.meta.Timestamp, common.GetTimestamp()}
	wanted, err := d.DiffReplica(obj.partition, splitObjectKey(obj.key)[2], tses)
	require.Nil(t, err)
	require.Equal(t, &WantedParts{false, true}, wanted)
}

func TestDiffReplica4(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD*2), "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	obj.Close()

	tses := &ObjectTimestamps{obj.meta.Timestamp, common.GetTimestamp()}
	wanted, err := d.DiffReplica(obj.partition, splitObjectKey(obj.key)[2], tses)
	require.Nil(t, err)
	require.Equal(t, &WantedParts{false, true}, wanted)
}

func TestDiffReplica5(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD*2), "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	obj.Close()

	tses := &ObjectTimestamps{incSeconds(obj.meta.Timestamp, -1), ""}
	wanted, err := d.DiffReplica(obj.partition, splitObjectKey(obj.key)[2], tses)
	require.Nil(t, err)
	require.Equal(t, &WantedParts{false, false}, wanted)
}

func TestDiffReplica6(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD*2), "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	obj.Close()

	vo := copyVanilla(obj)
	d.LoadObjectMeta(vo)
	d.CommitDeletion(vo)
	vo.Close()

	tses := &ObjectTimestamps{incSeconds(obj.meta.Timestamp, 10), ""}
	wanted, err := d.DiffReplica(obj.partition, splitObjectKey(obj.key)[2], tses)
	require.Nil(t, err)
	require.Equal(t, &WantedParts{true, true}, wanted)
}

func TestDiffReplica7(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD*2), "")
	feedObject(obj, d)
	d.CommitWrite(obj)
	obj.Close()

	vo := copyVanilla(obj)
	d.LoadObjectMeta(vo)
	d.CommitDeletion(vo)
	vo.Close()

	tses := &ObjectTimestamps{incSeconds(obj.meta.Timestamp, -1), ""}
	wanted, err := d.DiffReplica(obj.partition, splitObjectKey(obj.key)[2], tses)
	require.Nil(t, err)
	require.Equal(t, &WantedParts{false, false}, wanted)
}
