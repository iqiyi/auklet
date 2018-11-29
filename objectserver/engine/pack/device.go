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
	"os"
	"path/filepath"
	"sync"

	"github.com/tecbot/gorocksdb"
	"go.uber.org/zap"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
)

type PackDevice struct {
	driveRoot  string
	device     string
	policy     int
	hashPrefix string
	hashSuffix string
	objectsDir string
	bundles    map[string]*Bundle
	lock       sync.RWMutex
	db         *gorocksdb.DB
	wopt       *gorocksdb.WriteOptions
	ropt       *gorocksdb.ReadOptions
	wg         *sync.WaitGroup //garantee a clean exit
	km         *common.Kmutex
}

func NewPackDevice(device, driveRoot string, policy int) *PackDevice {
	var err error

	dd := filepath.Join(driveRoot, device)
	if err = os.MkdirAll(dd, 0755); err != nil {
		glogger.Error("unable to create device directory",
			zap.String("dir", dd),
			zap.Error(err))
		return nil
	}

	prefix, suffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		glogger.Error("unable to find hash prefix/suffix", zap.Error(err))
		return nil
	}

	op, dp := PackDevicePaths(device, driveRoot, policy)
	d := &PackDevice{
		driveRoot:  driveRoot,
		device:     device,
		policy:     policy,
		objectsDir: op,
		hashPrefix: prefix,
		hashSuffix: suffix,
		bundles:    make(map[string]*Bundle),
		wg:         &sync.WaitGroup{},
		km:         common.NewKmutex(),
	}

	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetWalSizeLimitMb(64)

	d.db, err = gorocksdb.OpenDb(opts, dp)
	if err != nil {
		glogger.Error("failed to open meta database",
			zap.String("database", dp),
			zap.Error(err))
		return nil
	}
	d.wopt = gorocksdb.NewDefaultWriteOptions()
	d.wopt.SetSync(true)
	d.ropt = gorocksdb.NewDefaultReadOptions()

	return d
}

func (d *PackDevice) tempDir() string {
	return filepath.Join(d.driveRoot, d.device, "tmp")
}

func (d *PackDevice) getBundle(partition string) (*Bundle, error) {
	if partition == "" {
		return nil, ErrEmptyPartition
	}

	d.lock.RLock()
	bundle, ok := d.bundles[partition]
	d.lock.RUnlock()
	if ok {
		return bundle, nil
	}

	d.lock.Lock()
	defer d.lock.Unlock()

	// Double check
	if bundle, ok = d.bundles[partition]; ok {
		return bundle, nil
	}

	bundle, err := OpenBundle(d.objectsDir, partition)
	if err != nil {
		return nil, err
	}
	d.bundles[partition] = bundle

	return bundle, nil
}

func (d *PackDevice) CloseVolume(partition string) error {
	d.lock.RLock()
	bundle, ok := d.bundles[partition]
	d.lock.RUnlock()
	if !ok {
		glogger.Info("bundle has been closed", zap.String("partition", partition))
		return nil
	}

	d.lock.Lock()
	delete(d.bundles, partition)
	d.lock.Unlock()

	return bundle.Cleanup()
}

func (d *PackDevice) Close() {
	glogger.Debug(
		"closing device", zap.String("name", d.device), zap.Int("policy", d.policy))
	// Make sure that the device could be close safely
	d.wg.Wait()

	d.db.Close()

	for _, bundle := range d.bundles {
		bundle.Cleanup()
	}

	glogger.Debug(
		"device closed", zap.String("name", d.device), zap.Int("policy", d.policy))
}

// **************************
// Re-implemented API
// ***************************
func (d *PackDevice) LoadObjectMeta(obj *PackObject) error {
	var err error
	// Reset the data structure if error occurs
	defer func() {
		if err != nil {
			obj.meta = nil
			obj.dataIndex = nil
			obj.metaIndex = nil
			obj.dMeta = nil
			obj.mMeta = nil
		}
	}()

	dataDBIdx, metaDBIdx, tsDBIdx, err := d.loadObjDBIndexes(obj)
	if err != nil {
		return err
	}

	if tsDBIdx != nil {
		obj.meta = tsDBIdx.Meta
		return nil
	}

	// Object not found
	if dataDBIdx == nil {
		return nil
	}

	obj.dataIndex = dataDBIdx.Index
	obj.exists = true
	obj.small = obj.dataIndex != nil
	obj.dMeta = dataDBIdx.Meta

	// Both dMeta, mMeta should be considered as const
	obj.meta = dataDBIdx.Meta.DeepCopy()

	// Stop if no extra meta needle found.
	if metaDBIdx == nil {
		return err
	}

	obj.metaIndex = metaDBIdx.Index
	obj.mMeta = metaDBIdx.Meta

	obj.meta.Timestamp = metaDBIdx.Meta.Timestamp

	// It is not easy to handle swift metadata. See
	// https://docs.openstack.org/swift/latest/development_middleware.html#swift-metadata
	for k, v := range metaDBIdx.Meta.SystemMeta {
		// Existing items will be kept.
		obj.meta.SystemMeta[k] = v
	}
	// Existing items won't be kept even there is no user meta in meta DB index
	obj.meta.UserMeta = map[string]string{}
	for k, v := range metaDBIdx.Meta.UserMeta {
		obj.meta.UserMeta[k] = v
	}

	return err
}

func (d *PackDevice) NewWriter(obj *PackObject) (*dataWriter, error) {
	// At this point, we are not able to ensure that we can get the
	// exact size
	if (obj.dataSize >= 0 && obj.dataSize <= NEEDLE_THRESHOLD) ||
		(obj.dataSize < 0 && gconf.PackChunkedObject) {
		obj.small = true
		return d.newSOWriter(obj)
	}

	obj.small = false
	return d.newLOWriter(obj)
}

func (d *PackDevice) NewMetaWriter(obj *PackObject) (*dataWriter, error) {
	// At this point, we already know if the object is a SO
	if obj.small {
		return d.newSOWriter(obj)
	}

	return d.newLOWriter(obj)
}

func (d *PackDevice) NewRangeReader(
	obj *PackObject, offset, size int64) (*dataReader, error) {
	if obj.small {
		return d.newSORangeReader(obj, offset, size)
	}

	return d.newLORangeReader(obj, offset, size)
}

func (d *PackDevice) NewReader(obj *PackObject) (*dataReader, error) {
	if obj.small {
		return d.newSORangeReader(obj, 0, obj.meta.DataSize)
	}

	return d.newLORangeReader(obj, 0, obj.meta.DataSize)
}

func (d *PackDevice) CommitWrite(obj *PackObject) error {
	if err := d.clearStaleDBIndexes(obj); err != nil {
		glogger.Error("unable to clean stale db indexes",
			zap.String("object", obj.name))
		return err
	}
	if obj.small {
		return d.commitSO(obj, DATA)
	}

	return d.commitLO(obj, DATA)
}

func (d *PackDevice) CommitUpdate(obj *PackObject) error {
	if obj.small {
		return d.commitSO(obj, META)
	}

	return d.commitLO(obj, META)
}

func (d *PackDevice) CommitDeletion(obj *PackObject) error {
	if err := d.clearDBIndexes(obj); err != nil {
		glogger.Error("unable to clear db indexes",
			zap.String("object", obj.name))
		return err
	}

	if obj.small {
		return d.deleteSO(obj)
	}

	return d.deleteLO(obj)
}
