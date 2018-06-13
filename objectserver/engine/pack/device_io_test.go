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
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/fs"
)

func TestSOWriter(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newPackSO("")
	w, err := d.newSOWriter(obj)
	require.Nil(t, err)

	_, ok := w.Writer.(*bytes.Buffer)
	require.True(t, ok)
}

func TestLOWriter(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newPackLO("")
	w, err := d.newLOWriter(obj)
	require.Nil(t, err)

	_, ok := w.Writer.(fs.AtomicFileWriter)
	require.True(t, ok)
}

func TestSORangeReader(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newPackSO("")
	obj.dataIndex = &NeedleIndex{Offset: 4096}
	r, err := d.newSORangeReader(obj, 0, obj.meta.DataSize)
	require.Nil(t, err)

	require.Nil(t, r.fd)
}

func TestLORangeReader(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newPackLO("")
	obj.dMeta = &ObjectMeta{Timestamp: obj.meta.Timestamp}
	obj.device = d

	require.Nil(t, mockLODataFile(obj))

	r, err := d.newLORangeReader(obj, 0, obj.meta.DataSize)
	require.Nil(t, err)

	require.NotNil(t, r.fd)
}

func TestCommitSO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackSO("")
	require.Nil(t, feedObject(obj, d))
	require.Nil(t, d.commitSO(obj, DATA))
	require.Nil(t, obj.Close())

	obj.device = d
	dp := generateLOFilePath(obj, obj.meta.Timestamp, DATA)
	require.True(t, fs.IsFileNotExist(dp))
}

func TestCommitLO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackLO("")
	require.Nil(t, feedObject(obj, d))
	require.Nil(t, d.commitLO(obj, DATA))
	require.Nil(t, obj.Close())

	obj.device = d
	dp := generateLOFilePath(obj, obj.meta.Timestamp, DATA)
	require.False(t, fs.IsFileNotExist(dp))
}

func TestWrongDataWriter1(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackSO("")
	require.Nil(t, feedObject(obj, d))
	require.Equal(t, d.commitLO(obj, DATA), ErrWrongDataWriter)
}

func TestWrongDataWriter2(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackLO("")
	require.Nil(t, feedObject(obj, d))
	require.Equal(t, d.commitSO(obj, DATA), ErrWrongDataWriter)
}

func TestLoadSODBIndex(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackSO("")
	require.Nil(t, feedObject(obj, d))
	require.Nil(t, d.commitSO(obj, DATA))
	require.Nil(t, obj.Close())

	o := &PackObject{
		key: obj.key,
	}

	dIdx, mIdx, tsIdx, err := d.loadObjDBIndexes(o)
	require.Nil(t, err)
	require.NotNil(t, dIdx)
	require.Nil(t, mIdx)
	require.Nil(t, tsIdx)

	require.NotNil(t, dIdx.Index)
	require.Equal(t,
		int64(SuperBlockDiskSize+NeedleHeaderSize), dIdx.Index.DataOffset)
	require.Equal(t, obj.meta.SystemMeta, dIdx.Meta.SystemMeta)
}

func TestLoadLODBIndex(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackLO("")
	require.Nil(t, feedObject(obj, d))
	require.Nil(t, d.commitLO(obj, DATA))
	require.Nil(t, obj.Close())

	o := &PackObject{
		key: obj.key,
	}

	dIdx, mIdx, tsIdx, err := d.loadObjDBIndexes(o)
	require.Nil(t, err)
	require.NotNil(t, dIdx)
	require.Nil(t, mIdx)
	require.Nil(t, tsIdx)

	require.Nil(t, dIdx.Index)
	require.Equal(t, obj.meta.SystemMeta, dIdx.Meta.SystemMeta)
}

func TestDeleteLO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackLO("")
	require.Nil(t, feedObject(obj, d))
	require.Nil(t, d.commitLO(obj, DATA))
	require.Nil(t, obj.Close())

	obj.device = d
	dp := generateLOFilePath(obj, obj.meta.Timestamp, DATA)
	require.False(t, fs.IsFileNotExist(dp))

	obj.meta.Timestamp = common.GetTimestamp()
	require.Nil(t, d.deleteLO(obj))
	// This is a little tricky. In the case of LO, the commitLO will
	// start a goroutine to clean the data files which is never guaranteed
	// to be started when the function return. So we need to yield the
	// cpu here
	//runtime.Gosched()
	// Gosched does not work in CI. Use Sleep instead
	time.Sleep(time.Millisecond * 10)
	require.True(t, fs.IsFileNotExist(dp))
	_, _, tsIdx, err := d.loadObjDBIndexes(obj)
	require.Nil(t, err)
	require.NotNil(t, tsIdx)
	tp := generateLOFilePath(obj, obj.meta.Timestamp, TOMBSTONE)
	require.False(t, fs.IsFileNotExist(tp))
}

func TestDeleteSO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackSO("")
	require.Nil(t, feedObject(obj, d))
	require.Nil(t, d.commitSO(obj, DATA))
	require.Nil(t, obj.Close())

	obj.meta.Timestamp = common.GetTimestamp()
	require.Nil(t, d.deleteSO(obj))
	_, _, tsIdx, err := d.loadObjDBIndexes(obj)
	require.Nil(t, err)
	require.NotNil(t, tsIdx)
}

func TestPersistSO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackSO("")
	require.Nil(t, feedObject(obj, d))
	require.Nil(t, d.commitSO(obj, DATA))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	oo := &PackObject{
		partition: obj.partition,
		dataIndex: obj.dataIndex,
		meta:      obj.meta,
	}

	r, err := dd.newSORangeReader(oo, 0, oo.meta.DataSize)
	require.Nil(t, err)
	hash := md5.New()
	io.Copy(hash, r)
	etag := hex.EncodeToString(hash.Sum(nil))
	require.Equal(t, obj.meta.SystemMeta[common.HEtag], etag)
}

func TestPersistLO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackLO("")
	require.Nil(t, feedObject(obj, d))
	require.Nil(t, d.commitLO(obj, DATA))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	oo := &PackObject{
		key:       obj.key,
		partition: obj.partition,
		dMeta:     &ObjectMeta{Timestamp: obj.meta.Timestamp},
		meta:      obj.meta,
	}

	r, err := dd.newLORangeReader(oo, 0, oo.meta.DataSize)
	require.Nil(t, err)
	hash := md5.New()
	io.Copy(hash, r)
	etag := hex.EncodeToString(hash.Sum(nil))
	require.Equal(t, obj.meta.SystemMeta[common.HEtag], etag)
}
