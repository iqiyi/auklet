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
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/fs"
)

type PartType string

const (
	DATA      PartType = "data"
	META      PartType = "meta"
	TOMBSTONE PartType = "ts"
)

// File above 4M will be save as standalone file
const (
	NEEDLE_THRESHOLD = 4 * 1024 * 1024
	RECLAIM_AGE      = 60 * 60 * 24 * 7
)

// ********************
// Object Read
// ********************
type dataReader struct {
	*io.SectionReader

	fd *os.File // underlying file
}

func (r *dataReader) Close() error {
	if r.fd != nil {
		return r.fd.Close()
	}

	return nil
}

func (d *PackDevice) newSORangeReader(
	obj *PackObject, offset, size int64) (*dataReader, error) {
	bundle, err := d.getBundle(obj.partition)
	if err != nil {
		return nil, err
	}

	off := obj.dataIndex.DataOffset + offset
	return &dataReader{io.NewSectionReader(bundle, off, size), nil}, nil
}

func (d *PackDevice) newLORangeReader(
	obj *PackObject, offset, size int64) (*dataReader, error) {
	hashDir := filepath.Join(d.objectsDir, obj.key)
	dp := filepath.Join(hashDir, fmt.Sprintf("%s.%s", obj.dMeta.Timestamp, DATA))
	f, err := os.Open(dp)
	if err != nil {
		glogger.Error("unable to open large object data file",
			zap.String("object", obj.name),
			zap.String("path", dp))
		return nil, err
	}

	return &dataReader{io.NewSectionReader(f, offset, size), f}, nil
}

func (d *PackDevice) getDBIndex(key string, ot PartType) (*DBIndex, error) {
	b, err := d.db.GetBytes(d.ropt, []byte(filepath.Join(key, string(ot))))
	if err != nil {
		glogger.Error("unable to retrieve db index",
			zap.String("object-key", key),
			zap.String("part-type", string(ot)))
		return nil, err
	}

	// db index not found
	if len(b) == 0 {
		return nil, nil
	}

	idx := new(DBIndex)
	if err = proto.Unmarshal(b, idx); err != nil {
		glogger.Error("unable to marshal db index",
			zap.String("object-key", key),
			zap.String("part-type", string(ot)),
			zap.Error(err))
		idx = nil
	}

	return idx, err
}

func (d *PackDevice) loadObjDBIndexes(obj *PackObject) (dataDBIndex,
	metaDBIndex, tsDBIndex *DBIndex, err error) {
	dataDBIndex, err = d.getDBIndex(obj.key, DATA)
	if err != nil {
		return
	}

	metaDBIndex, err = d.getDBIndex(obj.key, META)
	if err != nil {
		return
	}

	tsDBIndex, err = d.getDBIndex(obj.key, TOMBSTONE)

	return
}

// ********************
// Object Creation & Update
// ********************
type dataWriter struct {
	io.Writer
}

func (w *dataWriter) Close() error {
	if awf, ok := w.Writer.(fs.AtomicFileWriter); ok {
		return awf.Abandon()
	}

	return nil
}

func (d *PackDevice) newSOWriter(obj *PackObject) (*dataWriter, error) {
	bufSize := CalculateBufferSize(NeedleHeaderSize, obj.dataSize)
	buf := make([]byte, NeedleHeaderSize, bufSize)
	return &dataWriter{bytes.NewBuffer(buf)}, nil
}

func (d *PackDevice) newLOWriter(obj *PackObject) (*dataWriter, error) {
	tempDir := d.tempDir()
	hashDir := filepath.Join(d.objectsDir, obj.key)
	w, err := fs.NewAtomicFileWriter(tempDir, hashDir)
	if err != nil {
		glogger.Error("unable to create AtomicFileWriter",
			zap.String("name", obj.name))
	}

	return &dataWriter{w}, err
}

func (d *PackDevice) clearDataDBIndex(obj *PackObject) error {
	dKey := filepath.Join(obj.key, string(DATA))
	return d.db.Delete(d.wopt, []byte(dKey))
}

func (d *PackDevice) clearMetaDBIndex(obj *PackObject) error {
	mKey := filepath.Join(obj.key, string(META))
	return d.db.Delete(d.wopt, []byte(mKey))
}

func (d *PackDevice) clearTombstoneDBIndex(obj *PackObject) error {
	tsKey := filepath.Join(obj.key, string(TOMBSTONE))
	return d.db.Delete(d.wopt, []byte(tsKey))
}

func (d *PackDevice) clearStaleDBIndexes(obj *PackObject) error {
	// Tombstone found. Clear it before saving the new object.
	if !obj.exists && obj.meta != nil {
		if err := d.clearTombstoneDBIndex(obj); err != nil {
			glogger.Error("unable to delete tombstone",
				zap.String("object", obj.name), zap.Error(err))
			return err
		}
	}

	if obj.exists && obj.mMeta != nil {
		if err := d.clearMetaDBIndex(obj); err != nil {
			glogger.Error("unable to clear stale meta",
				zap.String("object", obj.name), zap.Error(err))
			return err
		}
	}

	return nil
}

// TODO: can we merge this with clearStaleDBIndexes ?
func (d *PackDevice) clearDBIndexes(obj *PackObject) error {
	if err := d.clearDataDBIndex(obj); err != nil {
		glogger.Error("unable to delete data db index",
			zap.String("object-key", obj.key))
		return err
	}

	if err := d.clearMetaDBIndex(obj); err != nil {
		glogger.Error("unable to delete meta db index",
			zap.String("object-key", obj.key))
		return err
	}

	return nil
}

func (d *PackDevice) saveDBIndex(obj *PackObject, ot PartType) error {
	idx := &DBIndex{
		Meta: obj.meta,
	}
	if ot == DATA {
		idx.Index = obj.dataIndex
	} else if ot == META {
		idx.Index = obj.metaIndex
	} else if ot == TOMBSTONE {
		idx.Index = nil
	} else {
		glogger.Panic("unknown type of db index",
			zap.String("dbindex", string(ot)))
	}

	b, err := proto.Marshal(idx)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%s", obj.key, ot)
	return d.db.Put(d.wopt, []byte(key), b)
}

func (d *PackDevice) commitLO(obj *PackObject, ot PartType) error {
	var err error
	var stale *PackObject
	// We don't need to deallocate an existing LO
	if obj.exists && obj.dataIndex != nil {
		stale = d.deepStaleObjCopy(obj)
		// Prune existing needle indexes
		obj.dataIndex = nil
		obj.metaIndex = nil
	}

	meta := make(map[string]string)

	for k, v := range obj.meta.SystemMeta {
		meta[k] = v
	}

	for k, v := range obj.meta.UserMeta {
		meta[k] = v
	}
	meta["name"] = obj.name
	meta[common.XTimestamp] = obj.meta.Timestamp

	if ot != DATA {
		obj.writer, err = d.newLOWriter(obj)
		if err != nil {
			glogger.Error("unable to create AtomicFileWriter",
				zap.String("object", obj.name), zap.Error(err))
			return err
		}
	}

	afw, ok := obj.writer.Writer.(fs.AtomicFileWriter)
	if !ok {
		return ErrWrongDataWriter
	}

	if err = WriteMetadata(afw.Fd(), meta); err != nil {
		glogger.Error("unable to write meta",
			zap.String("object", obj.name), zap.Error(err))
		return err
	}

	hashDir := filepath.Join(d.objectsDir, obj.key)
	dst := filepath.Join(hashDir, fmt.Sprintf("%s.%s", obj.meta.Timestamp, ot))
	if err = afw.Save(dst); err != nil {
		glogger.Error("unable to save AtomicFileWriter",
			zap.String("object", obj.name),
			zap.String("dst", dst),
			zap.Error(err))
		return err
	}

	if err = d.saveDBIndex(obj, ot); err != nil {
		glogger.Error("unable to save index", zap.String("object", obj.name))
		return err
	}

	go func() {
		d.wg.Add(1)
		defer d.wg.Done()

		HashCleanupListDir(hashDir, RECLAIM_AGE)
		if dir, err := os.OpenFile(hashDir, os.O_RDONLY, 0666); err == nil {
			dir.Sync()
			dir.Close()
		}
		// Non nil stale means it was a small object
		if stale != nil {
			glogger.Info("deallocating stale object",
				zap.String("object", stale.name),
				zap.Bool("small", stale.small))
			d.deallocateSO(stale, DATA)
		}
	}()

	return nil
}

func (d *PackDevice) deepStaleObjCopy(obj *PackObject) *PackObject {
	return &PackObject{
		name:      obj.name,
		key:       obj.key,
		partition: obj.partition,
		// N.B, we cannot copy the field directly because it has been
		// overriden during writer creation
		// TODO: this is not good, try to find some better way in the
		// future
		small:     obj.dataIndex != nil,
		dataIndex: obj.dataIndex,
		metaIndex: obj.metaIndex,
	}
}

func (d *PackDevice) commitSO(obj *PackObject, ot PartType) error {
	bundle, err := d.getBundle(obj.partition)
	if err != nil {
		glogger.Error("unable to find bundle",
			zap.String("partition", obj.partition), zap.Error(err))
		return err
	}

	// Saved for object deallocation during object override
	var stale *PackObject
	if obj.exists {
		stale = d.deepStaleObjCopy(obj)
		// This is a meta update, so don't deallocate data
		// and it is always safe no matter the object is SO or not.
		if ot == META {
			stale.dataIndex = nil
		}
	}

	if ot != DATA {
		obj.writer, err = d.newSOWriter(obj)
		if err != nil {
			glogger.Error("unable to create SOWriter",
				zap.String("object", obj.name), zap.Error(err))
			return err
		}
	}

	bundle.Lock()
	defer bundle.Unlock()

	// Ensure new data is always to append to the end and is aligned to 4K
	var offset int64
	if offset, err = bundle.Seek(0, io.SeekEnd); err != nil {
		glogger.Error("unable to seek to end of bundle file",
			zap.String("partition", obj.partition), zap.Error(err))
		return err
	}
	if offset%NeedleAlignment != 0 {
		return ErrNeedleNotAligned
	}

	// Roll back in case of any failure.
	// IMPORTANT!!!
	// Deferred function calls in Go are executed in Last In First Out order
	// after the surrounding function returns. This makes sure that this rollback
	// operation will be called before pack is released.
	defer func() {
		if err != nil {
			bundle.Truncate(offset)
		}
	}()

	// Only for saving space
	delete(obj.meta.UserMeta, common.XTimestamp)
	delete(obj.meta.UserMeta, "name")

	// *Append* meta to the buffer, namely after the data
	b, err := proto.Marshal(obj.meta)
	if err != nil {
		glogger.Error("unable to marshal object meta",
			zap.String("object", obj.name), zap.Error(err))
		return err
	}

	if _, err = obj.writer.Write(b); err != nil {
		glogger.Error("unable to write meta to buffer",
			zap.String("object", obj.name), zap.Error(err))
		return err
	}

	nh := &NeedleHeader{
		MagicNumber: NeedleMagicNumber,
		DataOffset:  offset + int64(NeedleHeaderSize),
		DataSize:    obj.dataSize,
	}
	nh.MetaOffset = nh.DataOffset + nh.DataSize
	nh.MetaSize = int32(len(b))
	nh.NeedleSize = CalculateDiskSize(NeedleHeaderSize, nh.DataSize, int32(len(b)))

	buf, ok := obj.writer.Writer.(*bytes.Buffer)
	if !ok {
		glogger.Error("data writer is not for small object")
		return ErrWrongDataWriter
	}
	nh.WriteToBuffer(buf.Bytes()[0:NeedleHeaderSize])

	// Pad the buffer with 0 to achieve the 4k alignment
	paddingSize := int(nh.NeedleSize - int64(NeedleHeaderSize) - int64(nh.MetaSize) - nh.DataSize)
	if _, err = obj.writer.Write(padding[:paddingSize]); err != nil {
		glogger.Error("unable to pad object to buffer",
			zap.String("object", obj.name), zap.Error(err))
		return err
	}

	// Flush the buffer to the bundle
	if _, err = bundle.Write(buf.Bytes()); err != nil {
		glogger.Error("unable to write object data",
			zap.String("object", obj.name), zap.Error(err))
		return err
	}

	nIndex := &NeedleIndex{
		Offset:     offset,
		Size:       nh.NeedleSize,
		DataOffset: nh.DataOffset,
		DataSize:   nh.DataSize,
		MetaOffset: nh.MetaOffset,
		MetaSize:   nh.MetaSize,
	}

	if ot == DATA {
		obj.dataIndex = nIndex
	} else {
		obj.metaIndex = nIndex
	}
	err = d.saveDBIndex(obj, ot)

	// Deallocate existing needles when overriding objects
	// We could have done this at the begining, however,
	// it is better to remove old objects only when new objects
	// are created successfully, IMHO.
	if stale != nil {
		glogger.Info("deallocating stale object",
			zap.String("object", stale.name),
			zap.Bool("small", stale.small),
			zap.String("part-type", string(ot)))
		if stale.small {
			go d.deallocateSO(stale, ot)
		} else {
			go d.deallocateLO(stale, ot)
		}
	}

	return err
}

// ********************
// Object deletion
// ********************
func (d *PackDevice) deallocateNeedles(
	bundle *Bundle, indexes ...*NeedleIndex) error {
	for _, oi := range indexes {
		if oi == nil {
			continue
		}
		if err := bundle.PunchHole(oi.Offset, oi.Size); err != nil {
			glogger.Error("unable to punch hole.",
				zap.Int64("offset", oi.Offset),
				zap.String("partition", bundle.partition))
			return err
		}
	}

	return nil
}

func (d *PackDevice) deallocateSO(obj *PackObject, ot PartType) error {
	bundle, err := d.getBundle(obj.partition)
	if err != nil {
		glogger.Error("unable to find bundle",
			zap.String("object", obj.name), zap.String("partition", obj.partition))
		return err
	}

	if ot == META {
		return d.deallocateNeedles(bundle, obj.metaIndex)
	}

	return d.deallocateNeedles(bundle, obj.metaIndex, obj.dataIndex)
}

func (d *PackDevice) deallocateLO(obj *PackObject, ot PartType) error {
	if ot == META {
		// There is not need to deallocate meta part of LO because the
		// HashCleanupListDir called in commitLO will delete stale files.
		return nil
	}

	var err error
	if obj.small {
		glogger.Fatal("impossible to deallocate small object here",
			zap.String("object", obj.name))
		return ErrWrongDeallocaion
	}
	hashDir := filepath.Join(d.objectsDir, obj.key)
	if err = os.RemoveAll(hashDir); err != nil {
		glogger.Error("unable to deallocate large object",
			zap.String("object", obj.name))
	}

	return err
}

func (d *PackDevice) deleteSO(obj *PackObject) error {
	var err error
	if err = d.saveDBIndex(obj, TOMBSTONE); err != nil {
		glogger.Error("unable to save tombstone index",
			zap.String("object", obj.name))
		return err
	}

	// deallocatedSO will try to clear the meta needle implicitly
	if err = d.deallocateSO(obj, DATA); err != nil {
		glogger.Error("unable to deallocate needle space",
			zap.String("object", obj.name))
	}

	return err
}

func (d *PackDevice) deleteLO(obj *PackObject) error {
	return d.commitLO(obj, TOMBSTONE)
}
