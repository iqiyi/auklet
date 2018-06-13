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

// Provide necessary API for object replicator
package pack

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/fs"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

const (
	// Because suffix is a hexadecimal string of size 3,
	// thus the maximum suffixes per partition is 4096
	MaxPartitionSuffixes = 4096
)

func (d *PackDevice) isSuffixExists(partition, suffix string) bool {
	prefix := []byte(fmt.Sprintf("/%s/%s/", partition, suffix))

	iter := d.db.NewIterator(d.ropt)
	defer iter.Close()
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		return true
	}

	return false
}

// The algorithm should work in product environment.
// Rough benchmark shows that it takes about 100ms to get the suffix
// list in a partition.
// To be honest, it is not efficient but it is simple.
// If it is inadequent, extra complex data structures should be maintained.
func (d *PackDevice) ListSuffixes(partition string) []string {
	suffixes := make([]string, 0, MaxPartitionSuffixes)
	for i := 0; i < MaxPartitionSuffixes; i++ {
		suffix := fmt.Sprintf("%03x", i)

		if d.isSuffixExists(partition, suffix) {
			suffixes = append(suffixes, suffix)
		}
	}

	return suffixes
}

// FIXME: identify the empty string because empty string has
// valid MD5 checksum.
func (d *PackDevice) CalculateSuffixHash(partition, suffix string,
	reclaimAge int64) (string, error) {
	h := md5.New()

	prefix := []byte(fmt.Sprintf("/%s/%s/", partition, suffix))

	iter := d.db.NewIterator(d.ropt)
	defer iter.Close()
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		b := iter.Value().Data()
		nMeta := new(DBIndex)
		if err := proto.Unmarshal(b, nMeta); err != nil {
			glogger.Error("unable to unmarshal needle meta",
				zap.String("object-key", string(iter.Key().Data())))
			return "", ErrDBIndexCorrupted
		}

		io.WriteString(h, nMeta.Meta.Timestamp)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// Return the absolute paths of partition directory, hashes.pkl and
// hashes.invalid.
func (d *PackDevice) hashesPaths(partition string) (string, string, string) {
	pd := filepath.Join(d.objectsDir, partition)
	hp := filepath.Join(pd, common.HASH_FILE)
	ip := filepath.Join(pd, common.HASH_INVALIDATIONS_FILE)

	return pd, hp, ip
}

// A port of from its Python counterpart.
// It is kind of complex so from Newton, they improve the hashes.pkl
// mechanism which requires change to the format of hashes.pkl file.
// NO plan to implement the new design in version 1 unless we have to.
func (d *PackDevice) GetHashes(partition string,
	recalculate []string,
	lsSuffixes bool,
	reclaimAge int64) (hashed int64, hashes map[string]string, err error) {

	defer func() {
		if err != nil {
			hashes = nil
		}
	}()

	partitionDir, pklPath, invalidPath := d.hashesPaths(partition)

	if fs.IsFileNotExist(partitionDir) {
		return
	}

	modified := false
	forceRewrite := false

	// TODO: is it ok to ignore consolidation when full list is required ?
	hashes, err = ConsolidateHashes(pklPath, invalidPath)
	if err != nil || len(hashes) == 0 {
		lsSuffixes = true
		forceRewrite = true
	}

	mtime, err := fs.GetFileMTime(pklPath)

	if err != nil && !os.IsNotExist(err) {
		glogger.Error("unable to get modification time of hashes pkl file",
			zap.String("path", pklPath))
		return
	}

	if lsSuffixes {
		suffs := d.ListSuffixes(partition)
		hashes = make(map[string]string)
		for _, suff := range suffs {
			hashes[suff] = ""
		}
	}

	for _, suff := range recalculate {
		hashes[suff] = ""
	}

	for suffix, hash := range hashes {
		if hash == "" {
			modified = true
			h, err := d.CalculateSuffixHash(partition, suffix, common.ONE_WEEK)
			if err == nil {
				hashes[suffix] = h
				hashed++
			} else {
				glogger.Error("unable to calculate the suffix hash",
					zap.String("partition", partition),
					zap.String("suffix", suffix))
			}
		}
	}

	// Reset ignorable error
	err = nil

	if modified {
		// Use anonymous function so that the logic of lock release is clear.
		// If done is true or any unexpected error occurs, then GetHashes should
		// *NOT* be called recursively.
		var done bool
		done, err = func() (bool, error) {
			pLock, e := fs.LockPath(partitionDir, time.Second*10)
			defer pLock.Close()
			// Unexpected error occurs, no need to call GetHashes recursively
			if e != nil {
				return true, e
			}

			mt, e := fs.GetFileMTime(pklPath)
			if !(forceRewrite || os.IsNotExist(e) || mtime == mt) {
				// If none of the conditions is met, then the hashes.pkl file shall
				// not be refreshed at the moment.
				// 1. A force rewrite is required
				// 2. hashes.pkl does not exist
				// 3. hashes.pkl has not been modified
				return false, e
			}

			e = SaveHashesPkl(hashes, pklPath, d.tempDir(), partitionDir)
			if e != nil {
				glogger.Error("unable to rewrite hashes.pkl",
					zap.String("partition", partition))
			}

			// If the SaveHashesPkl is successful, every thing works as expected.
			// Else unexpected error occurs.
			// So no matter pkl file gets refreshed successfully or not, we shall not
			// call GetHashes again.
			return true, e
		}()

		if !done {
			return d.GetHashes(partition, recalculate, lsSuffixes, reclaimAge)
		}
	}

	return
}

func (d *PackDevice) DiffReplica(partition, objHash string,
	timestamps *ObjectTimestamps) (*WantedParts, error) {
	wanted := &WantedParts{}

	obj := &PackObject{
		key:    generateKeyFromHash(partition, objHash),
		device: d,
	}
	dataDBIdx, metaDBIdx, tsDBIdx, err := d.loadObjDBIndexes(obj)
	if err != nil {
		return nil, err
	}

	if gconf.LazyMigration && tsDBIdx == nil && dataDBIdx == nil &&
		obj.Migrate() {

		dataDBIdx, metaDBIdx, tsDBIdx, err = d.loadObjDBIndexes(obj)
		if err != nil {
			return nil, err
		}
	}

	// The object has been deleted, we don't need any part.
	if tsDBIdx != nil && tsDBIdx.Meta.Timestamp >= timestamps.DataTimestamp {
		return wanted, nil
	}

	// We don't have the data part, so we need every part.
	if dataDBIdx == nil {
		wanted.Data = true
		wanted.Meta = true
		return wanted, nil
	}

	if timestamps.DataTimestamp > dataDBIdx.Meta.Timestamp {
		wanted.Data = true
	}

	metaTS := ""
	if metaDBIdx != nil {
		metaTS = metaDBIdx.Meta.Timestamp
	}
	if metaTS < timestamps.MetaTimestamp {
		wanted.Meta = true
	}

	return wanted, nil
}

func (d *PackDevice) ListSuffixTimestamps(partition, suffix string) (
	map[string]*ObjectTimestamps, error) {
	tses := make(map[string]*ObjectTimestamps)

	prefix := []byte(fmt.Sprintf("/%s/%s/", partition, suffix))
	iter := d.db.NewIterator(d.ropt)
	defer iter.Close()
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		key := string(iter.Key().Data())

		b := iter.Value().Data()
		nMeta := new(DBIndex)
		if err := proto.Unmarshal(b, nMeta); err != nil {
			glogger.Error("unable to unmarshal needle meta",
				zap.String("object-key", key))
			return nil, ErrDBIndexCorrupted
		}

		hash := splitObjectKey(key)[2]
		ts, ok := tses[hash]
		if !ok {
			ts = &ObjectTimestamps{}
			tses[hash] = ts
		}

		if strings.HasSuffix(key, "data") {
			ts.DataTimestamp = nMeta.Meta.Timestamp
		}

		if strings.HasSuffix(key, "meta") {
			ts.MetaTimestamp = nMeta.Meta.Timestamp
		}

		// If tombstone found, then set the data timestamp to
		// tombstone's timestamp.
		if strings.HasSuffix(key, "ts") {
			ts.DataTimestamp = nMeta.Meta.Timestamp
		}
	}

	return tses, nil
}

func (d *PackDevice) DeleteHandoff(partition string) error {
	// This method is invoked by rpc call which is not proteced
	// by graceful shutdown mechanism. So we use wait group
	// to make sure that this method won't be executed partially
	// when engine is being shutdown.
	// N.B. grpc server supports graceful shutdown actually. We don't
	// use it because some rpc calls could last for long time.
	// For example, audit a whole partition. So we use another
	// strategy to address the race. We only wait for those methods
	// that could change the db and volume files.
	d.wg.Add(1)
	defer d.wg.Done()

	partitionDir, _, invalidPath := d.hashesPaths(partition)
	mtime, err := fs.GetFileMTime(invalidPath)
	if err != nil {
		glogger.Error("unable to read modification time",
			zap.String("path", invalidPath))
		return err
	}

	prefix := []byte(fmt.Sprintf("/%s/", partition))
	iter := d.db.NewIterator(d.ropt)
	defer iter.Close()
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		key := iter.Key().Data()
		err := d.db.Delete(d.wopt, key)
		if err != nil {
			glogger.Error("unable to delete record from rocksdb",
				zap.String("key", string(key)))
			return err
		}
	}

	mtime2, err := fs.GetFileMTime(invalidPath)
	if err != nil {
		glogger.Error("unable to read modification time",
			zap.String("path", invalidPath))
		return err
	}

	if mtime2 > mtime {
		glogger.Error("bundle file has been modified",
			zap.String("partition", partition))
		return ErrBundleModifiedErr
	}

	if err = d.CloseVolume(partition); err != nil {
		glogger.Error("unable to close bundle file",
			zap.String("partition", partition), zap.Error(err))
		return err
	}

	return os.RemoveAll(partitionDir)
}
