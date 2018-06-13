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

	"github.com/stretchr/testify/require"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/fs"
)

func TestNewWriter1(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newPackSO("")
	w, err := d.NewWriter(obj)
	require.True(t, obj.small)
	require.Nil(t, err)
	_, ok := w.Writer.(*bytes.Buffer)
	require.True(t, ok)
}

func TestNewWriter2(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newPackLO("")
	w, err := d.NewWriter(obj)
	require.False(t, obj.small)
	require.Nil(t, err)
	_, ok := w.Writer.(fs.AtomicFileWriter)
	require.True(t, ok)
}

func TestNewWriter3(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newPackObject(-1, "")
	w, err := d.NewWriter(obj)
	require.False(t, obj.small)
	require.Nil(t, err)
	_, ok := w.Writer.(fs.AtomicFileWriter)
	require.True(t, ok)
}

func TestNewWriter4(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newPackObject(0, "")
	w, err := d.NewWriter(obj)
	require.True(t, obj.small)
	require.Nil(t, err)

	_, ok := w.Writer.(*bytes.Buffer)
	require.True(t, ok)
}

func TestCommitWriteSO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackSO("")
	require.Nil(t, feedObject(obj, d))

	require.Nil(t, d.CommitWrite(obj))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	oo := &PackObject{
		key:       obj.key,
		partition: obj.partition,
	}
	require.Nil(t, dd.LoadObjectMeta(oo))
	require.True(t, oo.small)

	r, err := dd.NewReader(oo)
	require.Nil(t, err)
	hash := md5.New()
	n, _ := io.Copy(hash, r)
	require.Equal(t, obj.meta.DataSize, n)
	etag := hex.EncodeToString(hash.Sum(nil))
	require.Equal(t, obj.meta.SystemMeta[common.HEtag], etag)
}

func TestCommitWriteLO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackLO("")
	require.Nil(t, feedObject(obj, d))

	require.Nil(t, d.CommitWrite(obj))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	oo := &PackObject{
		key:       obj.key,
		partition: obj.partition,
	}
	require.Nil(t, dd.LoadObjectMeta(oo))
	require.False(t, oo.small)

	r, err := dd.NewReader(oo)
	require.Nil(t, err)
	hash := md5.New()
	n, _ := io.Copy(hash, r)
	require.Equal(t, obj.meta.DataSize, n)
	etag := hex.EncodeToString(hash.Sum(nil))
	require.Equal(t, obj.meta.SystemMeta[common.HEtag], etag)
}

func TestCommitDeletionSO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackSO("")
	require.Nil(t, feedObject(obj, d))

	require.Nil(t, d.CommitWrite(obj))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	oo := &PackObject{
		key:       obj.key,
		partition: obj.partition,
	}
	require.Nil(t, dd.LoadObjectMeta(oo))
	require.True(t, oo.small)

	oo.meta.Timestamp = common.GetTimestamp()
	require.Nil(t, dd.CommitDeletion(oo))

	dIdx, _, tsIdx, err := dd.loadObjDBIndexes(obj)
	require.Nil(t, err)
	require.Nil(t, dIdx)
	require.NotNil(t, tsIdx)
}

func TestCommitDeletionLO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackLO("")
	require.Nil(t, feedObject(obj, d))

	require.Nil(t, d.CommitWrite(obj))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	oo := &PackObject{
		key:       obj.key,
		partition: obj.partition,
	}
	require.Nil(t, dd.LoadObjectMeta(oo))
	require.False(t, oo.small)

	oo.meta.Timestamp = common.GetTimestamp()
	require.Nil(t, dd.CommitDeletion(oo))

	dIdx, _, tsIdx, err := dd.loadObjDBIndexes(obj)
	require.Nil(t, err)
	require.Nil(t, dIdx)
	require.NotNil(t, tsIdx)
}

func TestCommitUpdateSO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackSO("")
	require.Nil(t, feedObject(obj, d))

	require.Nil(t, d.CommitWrite(obj))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	oo := &PackObject{
		key:       obj.key,
		partition: obj.partition,
	}
	require.Nil(t, dd.LoadObjectMeta(oo))
	require.True(t, oo.small)

	oo.writer, _ = dd.NewWriter(oo)

	oo.meta.Timestamp = common.GetTimestamp()
	oo.meta.UserMeta = map[string]string{"X-Object-Meta-Owner": "IQIYI"}
	require.Nil(t, dd.CommitUpdate(oo))

	dIdx, mIdx, _, err := dd.loadObjDBIndexes(obj)
	require.Nil(t, err)
	require.NotNil(t, dIdx)
	require.NotNil(t, mIdx)
}

func TestCommitUpdateLO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newPackLO("")
	require.Nil(t, feedObject(obj, d))

	require.Nil(t, d.CommitWrite(obj))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	oo := &PackObject{
		key:       obj.key,
		partition: obj.partition,
	}
	require.Nil(t, dd.LoadObjectMeta(oo))
	require.False(t, oo.small)

	oo.meta.Timestamp = common.GetTimestamp()
	oo.meta.UserMeta = map[string]string{"X-Object-Meta-Owner": "IQIYI"}
	require.Nil(t, dd.CommitUpdate(oo))

	dIdx, mIdx, _, err := dd.loadObjDBIndexes(obj)
	require.Nil(t, err)
	require.NotNil(t, dIdx)
	require.NotNil(t, mIdx)
}
