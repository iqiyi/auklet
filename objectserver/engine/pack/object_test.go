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
	"crypto/md5"
	"encoding/hex"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iqiyi/auklet/common"
)

func TestObjectCommitSO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newVanillaObject()
	obj.device = d

	require.Nil(t, d.LoadObjectMeta(obj))
	require.False(t, obj.exists)

	w, err := obj.SetData(rand.Int63n(NEEDLE_THRESHOLD))
	require.Nil(t, err)

	data := generateData(obj.dataSize)
	_, err = w.Write(data)
	require.Nil(t, err)

	metadata := map[string]string{
		"name":                obj.name,
		common.XTimestamp:     common.GetTimestamp(),
		common.HContentType:   common.VOctetStream,
		common.HContentLength: strconv.FormatInt(obj.dataSize, 10),
		common.HEtag:          bytesMd5(data),
	}

	require.Nil(t, obj.Commit(metadata))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	o1 := copyVanilla(obj)
	o1.device = dd
	require.Nil(t, dd.LoadObjectMeta(o1))
	require.True(t, o1.exists)
	hash := md5.New()
	n, err := o1.Copy(hash)
	require.Equal(t, obj.meta.DataSize, n)
	require.Nil(t, err)
	etag := hex.EncodeToString(hash.Sum(nil))
	require.Equal(t, obj.meta.SystemMeta[common.HEtag], etag)
	require.Nil(t, o1.Close())
	dd.Close()
}

func TestObjectCommitLO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newVanillaObject()
	obj.device = d

	require.Nil(t, d.LoadObjectMeta(obj))
	require.False(t, obj.exists)

	w, err := obj.SetData(
		int64(common.RandIntInRange(NEEDLE_THRESHOLD+1, SIZE_1M*8)))
	require.Nil(t, err)

	data := generateData(obj.dataSize)
	_, err = w.Write(data)
	require.Nil(t, err)

	metadata := map[string]string{
		"name":                obj.name,
		common.XTimestamp:     common.GetTimestamp(),
		common.HContentType:   common.VOctetStream,
		common.HContentLength: strconv.FormatInt(obj.dataSize, 10),
		common.HEtag:          bytesMd5(data),
	}

	require.Nil(t, obj.Commit(metadata))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	o1 := copyVanilla(obj)
	o1.device = dd
	require.Nil(t, dd.LoadObjectMeta(o1))
	require.True(t, o1.exists)
	hash := md5.New()
	n, err := o1.Copy(hash)
	require.Equal(t, obj.meta.DataSize, n)
	require.Nil(t, err)
	etag := hex.EncodeToString(hash.Sum(nil))
	require.Equal(t, obj.meta.SystemMeta[common.HEtag], etag)
	require.Nil(t, o1.Close())
	dd.Close()
}

func TestObjectDeleteSO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newVanillaObject()
	obj.device = d

	require.Nil(t, d.LoadObjectMeta(obj))
	require.False(t, obj.exists)

	w, err := obj.SetData(rand.Int63n(NEEDLE_THRESHOLD))
	require.Nil(t, err)

	data := generateData(obj.dataSize)
	_, err = w.Write(data)
	require.Nil(t, err)

	meta := map[string]string{
		"name":                obj.name,
		common.XTimestamp:     common.GetTimestamp(),
		common.HContentType:   common.VOctetStream,
		common.HContentLength: strconv.FormatInt(obj.dataSize, 10),
		common.HEtag:          bytesMd5(data),
	}

	require.Nil(t, obj.Commit(meta))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	o1 := copyVanilla(obj)
	o1.device = dd
	require.Nil(t, dd.LoadObjectMeta(o1))
	require.True(t, o1.exists)

	tsMeta := map[string]string{
		"name":            obj.name,
		common.XTimestamp: common.GetTimestamp(),
	}
	require.Nil(t, o1.Delete(tsMeta))
	require.Nil(t, o1.Close())

	o2 := copyVanilla(obj)
	o2.device = dd

	require.Nil(t, dd.LoadObjectMeta(o2))
	require.False(t, o2.exists)
}

func TestObjectDeleteLO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newVanillaObject()
	obj.device = d

	require.Nil(t, d.LoadObjectMeta(obj))
	require.False(t, obj.exists)

	w, err := obj.SetData(
		int64(common.RandIntInRange(NEEDLE_THRESHOLD+1, SIZE_1M*8)))
	require.Nil(t, err)

	data := generateData(obj.dataSize)
	_, err = w.Write(data)
	require.Nil(t, err)

	meta := map[string]string{
		"name":                obj.name,
		common.XTimestamp:     common.GetTimestamp(),
		common.HContentType:   common.VOctetStream,
		common.HContentLength: strconv.FormatInt(obj.dataSize, 10),
		common.HEtag:          bytesMd5(data),
	}

	require.Nil(t, obj.Commit(meta))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	o1 := copyVanilla(obj)
	o1.device = dd
	require.Nil(t, dd.LoadObjectMeta(o1))
	require.True(t, o1.exists)

	tsMeta := map[string]string{
		"name":            obj.name,
		common.XTimestamp: common.GetTimestamp(),
	}
	require.Nil(t, o1.Delete(tsMeta))
	require.Nil(t, o1.Close())

	o2 := copyVanilla(obj)
	o2.device = dd

	require.Nil(t, dd.LoadObjectMeta(o2))
	require.False(t, o2.exists)
}

func TestObjectUpdateSO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newVanillaObject()
	obj.device = d

	require.Nil(t, d.LoadObjectMeta(obj))
	require.False(t, obj.exists)

	w, err := obj.SetData(rand.Int63n(NEEDLE_THRESHOLD))
	require.Nil(t, err)

	data := generateData(obj.dataSize)
	_, err = w.Write(data)
	require.Nil(t, err)

	meta := map[string]string{
		"name":                obj.name,
		common.XTimestamp:     common.GetTimestamp(),
		common.HContentType:   common.VOctetStream,
		common.HContentLength: strconv.FormatInt(obj.dataSize, 10),
		common.HEtag:          bytesMd5(data),
	}

	require.Nil(t, obj.Commit(meta))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	o1 := copyVanilla(obj)
	o1.device = dd
	require.Nil(t, dd.LoadObjectMeta(o1))
	require.True(t, o1.exists)
	meta2 := map[string]string{"X-Object-Meta-Owner": "IQIYI"}
	require.Nil(t, o1.CommitMeta(meta2))
	require.Nil(t, o1.Close())

	o2 := copyVanilla(obj)
	o2.device = dd
	require.Nil(t, dd.LoadObjectMeta(o2))
	require.NotNil(t, o2.dMeta)
	require.NotNil(t, o2.mMeta)
	require.Nil(t, o2.Close())
}

func TestObjectUpdateLO(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)
	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)

	obj := newVanillaObject()
	obj.device = d

	require.Nil(t, d.LoadObjectMeta(obj))
	require.False(t, obj.exists)

	w, err := obj.SetData(
		int64(common.RandIntInRange(NEEDLE_THRESHOLD+1, SIZE_1M*8)))
	require.Nil(t, err)

	data := generateData(obj.dataSize)
	_, err = w.Write(data)
	require.Nil(t, err)

	metadata := map[string]string{
		"name":                obj.name,
		common.XTimestamp:     common.GetTimestamp(),
		common.HContentType:   common.VOctetStream,
		common.HContentLength: strconv.FormatInt(obj.dataSize, 10),
		common.HEtag:          bytesMd5(data),
	}

	require.Nil(t, obj.Commit(metadata))
	require.Nil(t, obj.Close())
	d.Close()

	dd := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	o1 := copyVanilla(obj)
	o1.device = dd
	require.Nil(t, dd.LoadObjectMeta(o1))
	require.True(t, o1.exists)
	meta2 := map[string]string{"X-Object-Meta-Owner": "IQIYI"}
	require.Nil(t, o1.CommitMeta(meta2))
	require.Nil(t, o1.Close())

	o2 := copyVanilla(obj)
	o2.device = dd
	require.Nil(t, dd.LoadObjectMeta(o2))

	require.NotNil(t, o2.dMeta)
	require.NotNil(t, o2.mMeta)
	require.Nil(t, o2.Close())
}
