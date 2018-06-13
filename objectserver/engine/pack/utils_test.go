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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateKeyFromHash(t *testing.T) {
	key := generateKeyFromHash("0", "abcdefghijklmnopqrstuvwxyzabcdef")
	require.Equal(t, "/0/def/abcdefghijklmnopqrstuvwxyzabcdef", key)
}

func TestGenerateKey(t *testing.T) {
	prefix, suffix := "changeme", "emegnahc"
	key := generateObjectKey(prefix, suffix, "/a/b/c", "0")
	require.Equal(t, "/0/dd3/91b5d6c35969c3db91e267a85bdbbdd3", key)
}

func TestSplitObjectKey(t *testing.T) {
	key := "/0/dd3/91b5d6c35969c3db91e267a85bdbbdd3"
	expected := []string{"0", "dd3", "91b5d6c35969c3db91e267a85bdbbdd3"}
	actual := splitObjectKey(key)
	require.Equal(t, expected, actual)
}

func TestIsObjectSystemMeta(t *testing.T) {
	require.True(t, isObjectSystemMeta("x-object-sysmeta-tag"))
	require.True(t, isObjectSystemMeta("X-Object-Sysmeta-Tag"))
	require.True(t, isObjectSystemMeta("X-object-sysmeta-Tag"))
}

func TestPackDevicePaths1(t *testing.T) {
	root := "/srv/not_exists"
	objPath, dbPath := PackDevicePaths("not_real_disk", root, 0)
	require.Equal(t, "/srv/not_exists/not_real_disk/objects", objPath)
	require.Equal(t, "/srv/not_exists/not_real_disk/pack-meta", dbPath)
}

func TestPackDevicePaths2(t *testing.T) {
	root := "/srv/not_exists"
	objPath, dbPath := PackDevicePaths("not_real_disk", root, 2)
	require.Equal(t, "/srv/not_exists/not_real_disk/objects-2", objPath)
	require.Equal(t, "/srv/not_exists/not_real_disk/pack-meta-2", dbPath)
}

func TestQuarantineDir1(t *testing.T) {
	root := "/srv/not_exists"
	dev := "not_real_disk"
	qd := QuarantineDir(root, dev, 0)
	require.Equal(t, "/srv/not_exists/not_real_disk/quarantined/objects", qd)
}

func TestQuarantineDir2(t *testing.T) {
	root := "/srv/not_exists"
	dev := "not_real_disk"
	qd := QuarantineDir(root, dev, 3)
	require.Equal(t, "/srv/not_exists/not_real_disk/quarantined/objects-3", qd)
}

func TestInvalidHash(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newVanillaObject()
	obj.device = d

	dir := filepath.Join(obj.device.objectsDir, obj.key)
	require.Nil(t, InvalidateHash(dir))
}

func TestLoadInvalidHash(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newVanillaObject()
	obj.device = d

	dir := filepath.Join(obj.device.objectsDir, obj.key)
	require.Nil(t, InvalidateHash(dir))
	d.Close()

	_, _, invalid := d.hashesPaths(obj.partition)
	suffixes, err := LoadInvalidSuffixes(invalid)
	require.Nil(t, err)
	require.Equal(t, splitObjectKey(obj.key)[1], suffixes[0])
}

func TestSaveHashPkl(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	pd, pkl, _ := d.hashesPaths("0")
	td := d.tempDir()
	hashes := map[string]string{"abc": "abcdefghijklmnopqrstuvwxyzabcdef"}
	require.Nil(t, SaveHashesPkl(hashes, pkl, td, pd))
}

func TestLoadHashPkl(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	pd, pkl, _ := d.hashesPaths("0")
	td := d.tempDir()
	hashes := map[string]string{
		"abc": "abcdefghijklmnopqrstuvwxyzabcdef",
		"bcd": "defabcdefghijklmnopqrstuvwxyzabc",
	}

	SaveHashesPkl(hashes, pkl, td, pd)
	h, err := LoadPklHashes(pkl)
	require.Nil(t, err)
	require.Equal(t, hashes, h)
}

func TestConsolidateHashes1(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newVanillaObject()
	obj.device = d

	dir := filepath.Join(obj.device.objectsDir, obj.key)
	require.Nil(t, InvalidateHash(dir))
	d.Close()

	_, pkl, invalid := d.hashesPaths(obj.partition)
	_, err = ConsolidateHashes(pkl, invalid)
	require.Nil(t, err)
	require.Equal(t, int64(0), fileSize(invalid))
}

func TestConsolidateHashes2(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(root)

	d := NewPackDevice(PACK_DEVICE, root, PACK_POLICY_INDEX)
	obj := newVanillaObject()
	obj.device = d

	dir := filepath.Join(obj.device.objectsDir, obj.key)
	require.Nil(t, InvalidateHash(dir))
	d.Close()

	pd, pkl, invalid := d.hashesPaths(obj.partition)
	tmp := d.tempDir()
	SaveHashesPkl(map[string]string{"abc": "abcdefghijklmnopqrstuvwxyzabcdef"}, pkl, tmp, pd)

	hashes, err := ConsolidateHashes(pkl, invalid)
	require.Nil(t, err)
	require.Equal(t, int64(0), fileSize(invalid))

	require.Equal(t, hashes["abc"], "abcdefghijklmnopqrstuvwxyzabcdef")
	h, ok := hashes[splitObjectKey(obj.key)[1]]
	require.True(t, ok)
	require.Equal(t, "", h)
}
