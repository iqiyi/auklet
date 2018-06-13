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

package pack

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/fs"

	"go.uber.org/zap"
)

type PackObject struct {
	meta     *ObjectMeta
	metadata map[string]string

	name      string
	key       string
	exists    bool
	small     bool
	partition string
	device    *PackDevice
	reader    *dataReader
	writer    *dataWriter
	// TODO: can we remove this filed ?
	dataSize int64

	dataIndex *NeedleIndex
	metaIndex *NeedleIndex

	dMeta *ObjectMeta
	mMeta *ObjectMeta

	asyncWG *sync.WaitGroup
}

func (o *PackObject) Metadata() map[string]string {
	return o.metadata
}

func (o *PackObject) ContentLength() int64 {
	return o.meta.DataSize
}

func (o *PackObject) Quarantine() error {
	return errors.New("Not Implemented")
}

func (o *PackObject) Exists() bool {
	return o.exists
}

func (o *PackObject) Copy(dsts ...io.Writer) (int64, error) {
	var err error
	o.reader, err = o.device.NewReader(o)
	if err != nil {
		glogger.Error("unable to create object reader",
			zap.String("object", o.name), zap.Error(err))
		return 0, err
	}
	return io.Copy(io.MultiWriter(dsts...), o.reader)
}

func (o *PackObject) CopyRange(w io.Writer, start int64, end int64) (int64, error) {
	var err error
	o.reader, err = o.device.NewRangeReader(o, start, end-start)
	if err != nil {
		glogger.Error("unable to create object range reader",
			zap.String("object", o.name), zap.Error(err))
		return 0, err
	}
	return io.Copy(w, o.reader)
}

func (o *PackObject) Repr() string {
	return fmt.Sprintf("object %s at partition %s", o.name, o.partition)
}

// N.B. CommitMeta shall NOT call this method as NewWriter will
// choose the Writer between LO and SO based on the size.
func (o *PackObject) SetData(size int64) (io.Writer, error) {
	o.dataSize = size
	w, err := o.device.NewWriter(o)
	o.writer = w
	return w, err
}

// Only for saving new object or delete an object.
// Meta update will not call this method
// We don't need worry chunked-ecoding is used or not because at this
// point we are able to learn the exact size of the body
func (o *PackObject) populateObjectMeta(metadata map[string]string) {
	if metadata == nil || len(metadata) == 0 {
		return
	}

	if metadata[common.HContentLength] != "" {
		// There should be no error here
		size, err := strconv.ParseInt(metadata[common.HContentLength], 10, 64)
		if err != nil {
			glogger.Fatal("unable to parse the content length",
				zap.String("string", metadata[common.HContentLength]))
		}
		o.dataSize = size
	}

	o.meta = &ObjectMeta{
		Name:       o.name,
		DataSize:   o.dataSize,
		Timestamp:  metadata[common.XTimestamp],
		UserMeta:   make(map[string]string),
		SystemMeta: make(map[string]string),
	}

	// There are 2 kinds of system metadata
	// 1. immutable ones such as content-type, content-length and etag
	// 2. ones that begins with 'x-object-sysmeta-'
	// all the rest are considered as user meta
	for k, v := range metadata {
		if isObjectSystemMeta(k) {
			o.meta.SystemMeta[k] = v
		} else {
			o.meta.UserMeta[k] = v
		}
	}
}

func (o *PackObject) Commit(metadata map[string]string) error {
	o.populateObjectMeta(metadata)

	if err := o.device.CommitWrite(o); err != nil {
		return err
	}

	go func() {
		o.asyncWG.Add(1)
		defer o.asyncWG.Done()
		InvalidateHash(filepath.Join(o.device.objectsDir, o.key))
	}()

	return nil
}

func (o *PackObject) Delete(metadata map[string]string) error {
	o.populateObjectMeta(metadata)

	if err := o.device.CommitDeletion(o); err != nil {
		return err
	}

	go func() {
		o.asyncWG.Add(1)
		defer o.asyncWG.Done()
		InvalidateHash(filepath.Join(o.device.objectsDir, o.key))
	}()

	return nil
}

// Handing swift meta is not easy. See
// https://docs.openstack.org/swift/latest/development_middleware.html#swift-metadata
func (o *PackObject) CommitMeta(metadata map[string]string) error {
	// We need to keep existing system metas while clear duplicated ones
	for k, v := range o.meta.SystemMeta {
		if o.dMeta.SystemMeta[k] == v {
			delete(o.meta.SystemMeta, k)
		}
	}
	// POST request will remove all existing user meta whether there is
	// new user meta or not
	o.meta.UserMeta = map[string]string{}
	// At this point, we can at least ensure that
	// 1. every item is allowed, or it would be filtered by object server
	// 2. if an item is a system meta, it must begin with 'x-object-sysmeta-'
	// 3. all the rest are user metadatas
	for k, v := range metadata {
		if common.IsSysMeta("object", k) {
			if o.dMeta.SystemMeta[k] != v {
				o.meta.SystemMeta[k] = v
			}
		} else {
			o.meta.UserMeta[k] = v
		}
	}

	o.meta.Timestamp = metadata[common.XTimestamp]
	if err := o.device.CommitUpdate(o); err != nil {
		return err
	}

	go func() {
		o.asyncWG.Add(1)
		defer o.asyncWG.Done()

		InvalidateHash(filepath.Join(o.device.objectsDir, o.key))
	}()

	return nil
}

func (o *PackObject) objectFiles(dir string) (data, meta, tombstone string) {
	all, err := fs.ReadDirNames(dir)
	if err != nil {
		return
	}

	// Iterate tombstone(.ts), meta(.meta), data(.data) in order
	for idx := len(all) - 1; idx >= 0; idx-- {
		f := all[idx]
		ap := filepath.Join(dir, f)

		if strings.HasSuffix(f, ".ts") {
			tombstone = ap
			return
		}
		if strings.HasSuffix(f, ".meta") {
			meta = ap
		} else if strings.HasSuffix(f, ".data") {
			data = ap
		}
	}

	return
}

func (o *PackObject) Migrate() bool {
	df, mf, tf := o.objectFiles(filepath.Join(o.device.objectsDir, o.key))

	if tf != "" {
		tm, err := ReadMetadata(tf)
		if err != nil {
			glogger.Error("unable to read attributes from tombstone file",
				zap.String("object", o.name), zap.Error(err))
			return false
		}
		o.populateObjectMeta(tm)
		if err = o.device.saveDBIndex(o, TOMBSTONE); err != nil {
			glogger.Error("unable to save db index",
				zap.String("object", o.name),
				zap.String("part-type", string(TOMBSTONE)),
				zap.Error(err))
			return false
		}

		return true
	}

	if df == "" {
		return false
	}

	dm, err := ReadMetadata(df)
	if err != nil {
		glogger.Error("unable to read attributes from data file",
			zap.String("object", o.name), zap.Error(err))
		return false
	}
	o.populateObjectMeta(dm)
	if err = o.device.saveDBIndex(o, DATA); err != nil {
		glogger.Error("unable to save db index",
			zap.String("object", o.name), zap.Error(err))
		return false
	}

	if mf == "" {
		return true
	}

	mm, err := ReadMetadata(mf)
	if err != nil {
		glogger.Error("unable to read attributes from meta file",
			zap.String("object", o.name), zap.Error(err))
		return false
	}

	o.populateObjectMeta(mm)
	if err = o.device.saveDBIndex(o, META); err != nil {
		glogger.Error("unable to save db index",
			zap.String("object", o.name), zap.Error(err))
		return false
	}

	return true
}

// Close releases any resources used by the instance of PackObject
// This method is very important. If we don't close the reader/writer
// explicitly, file descriptors may be leaked.
func (o *PackObject) Close() error {
	if o.reader != nil {
		// I can't imagine an object with both reader/writer set right now.
		// That is why it returns fast here.
		return o.reader.Close()
	}

	if o.writer != nil {
		return o.writer.Close()
	}

	return nil
}
