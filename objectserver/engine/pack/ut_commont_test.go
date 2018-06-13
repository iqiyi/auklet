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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
)

const (
	PACK_POLICY_INDEX = 0
	PACK_DEVICE       = "not_real_disk"

	SIZE_1K = 1024
	SIZE_1M = 1024 * 1024

	PACK_SMALL_OBJ_LIMIT = SIZE_1K * 100
)

func init() {
	rand.Seed(time.Now().UnixNano())

	gconf = &PackConfig{}

	var err error
	glogger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
}

func bytesMd5(data []byte) string {
	hasher := md5.New()
	hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil))
}

func newTestPackDevice(device, root string) *PackDevice {
	policy := 0
	dev := NewPackDevice(device, root, policy)
	return dev
}

func newVanillaObject() *PackObject {
	partition := strconv.Itoa(int(rand.Int31()))
	obj := &PackObject{
		name:      fmt.Sprintf("/a/c/o-%010d", rand.Int31()),
		partition: partition,
		asyncWG:   &sync.WaitGroup{},
	}

	prefix, suffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		panic(err)
	}
	obj.key = generateObjectKey(prefix, suffix, obj.name, obj.partition)

	return obj
}
func copyVanilla(obj *PackObject) *PackObject {
	return &PackObject{
		name:      obj.name,
		key:       obj.key,
		partition: obj.partition,
		asyncWG:   obj.asyncWG,
	}

}

func newPackObject(size int64, partition string) *PackObject {
	if partition == "" {
		partition = strconv.Itoa(int(rand.Int31()))
	}
	obj := &PackObject{
		name:      fmt.Sprintf("/a/c/o-%010d", rand.Int31()),
		partition: partition,
		dataSize:  size,
		small:     size <= NEEDLE_THRESHOLD,
	}

	prefix, suffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		panic(err)
	}
	obj.key = generateObjectKey(prefix, suffix, obj.name, obj.partition)

	obj.meta = &ObjectMeta{
		Name:       obj.name,
		DataSize:   size,
		Timestamp:  common.GetTimestamp(),
		UserMeta:   make(map[string]string),
		SystemMeta: make(map[string]string),
	}
	obj.meta.SystemMeta[common.HContentType] = common.VOctetStream

	return obj
}

func newPackSO(partition string) *PackObject {
	obj := newPackObject(rand.Int63n(NEEDLE_THRESHOLD), partition)
	obj.small = true
	return obj
}

func newPackLO(partition string) *PackObject {
	obj := newPackObject(
		int64(common.RandIntInRange(NEEDLE_THRESHOLD+1, SIZE_1M*8)), partition)
	obj.small = false
	return obj
}

func generateLOFilePath(obj *PackObject, timestamp string, part PartType) string {
	hashDir := filepath.Join(obj.device.objectsDir, obj.key)
	return filepath.Join(hashDir, fmt.Sprintf("%s.%s", timestamp, part))
}

func mockLODataFile(obj *PackObject) error {
	hashDir := filepath.Join(obj.device.objectsDir, obj.key)
	if err := os.MkdirAll(hashDir, 0755); err != nil {
		return err
	}

	dp := filepath.Join(hashDir, fmt.Sprintf("%s.%s", obj.dMeta.Timestamp, DATA))
	f, err := os.Create(dp)
	if err != nil {
		return err
	}
	defer f.Close()

	data := make([]byte, obj.dataSize)
	if _, err := rand.Read(data); err != nil {
		return err
	}

	_, err = f.Write(data)

	return err
}

func generateData(size int64) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func feedObject(obj *PackObject, device *PackDevice) error {
	data := generateData(obj.dataSize)
	digest := md5.Sum(data)
	obj.meta.SystemMeta[common.HEtag] = hex.EncodeToString(digest[:])
	obj.meta.SystemMeta[common.HContentLength] = strconv.Itoa(len(data))
	obj.meta.SystemMeta[common.XTimestamp] = common.GetTimestamp()

	var w *dataWriter
	var err error
	if obj.small {
		w, err = device.newSOWriter(obj)
	} else {
		w, err = device.newLOWriter(obj)
	}
	if err != nil {
		return err
	}

	obj.writer = w

	_, err = w.Write(data)
	return err
}

func fileSize(filePath string) int64 {
	info, err := os.Stat(filePath)
	if err != nil {
		return -1
	}

	return info.Size()
}

func incSeconds(time string, seconds int) string {
	t, _ := strconv.ParseFloat(time, 64)
	return strconv.FormatFloat(t+float64(seconds), 'f', 6, 64)
}
