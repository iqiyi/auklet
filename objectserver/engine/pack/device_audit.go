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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/iqiyi/auklet/common"
)

type AuditStat struct {
	ProcessedBytes int64
	ProcessedFiles int64
	Quarantines    int64
	Errors         int64
}

const (
	NANO            = 1e9
	FILES_INCREMENT = 1
	LIMITER_BUFFER  = 5
)

// Go implementation of the Python version rate limiter.
// https://github.com/openstack/swift/blob/2.3.0/swift/common/utils.py#L2167
func (d *PackDevice) limitAuditRate(filesQuota int64, rate int64,
	increment int64, buffer int64) int64 {
	if rate == 0 {
		return filesQuota
	}
	timePerRequest := NANO * increment / rate // time per request in nano second
	now := time.Now().UnixNano()
	if now-filesQuota > buffer*NANO {
		filesQuota = now
	} else if filesQuota-now > timePerRequest {
		duration := time.Duration(filesQuota - now)
		time.Sleep(time.Nanosecond * duration)
	}

	return filesQuota + timePerRequest
}

func (d *PackDevice) AuditPartition(partition string) (*AuditStat, error) {
	stat := &AuditStat{}
	filesQuota := int64(0)
	bytesQuota := int64(0)

	prefix := []byte(fmt.Sprintf("/%s/", partition))
	iter := d.db.NewIterator(d.ropt)
	defer iter.Close()
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		key := string(iter.Key().Data())
		if !strings.HasSuffix(key, "/data") {
			// Only data are audited at the moment
			continue
		}

		filesQuota = d.limitAuditRate(filesQuota, gconf.AuditorFPS,
			FILES_INCREMENT, LIMITER_BUFFER)

		b := iter.Value().Data()
		dbIndex := new(DBIndex)
		if err := proto.Unmarshal(b, dbIndex); err != nil {
			glogger.Error("unable to unmarshal object db dbIndex ",
				zap.String("object-key", key), zap.Error(err))
			stat.Errors++
			continue
		}

		obj := &PackObject{
			name:      dbIndex.Meta.Name,
			meta:      dbIndex.Meta,
			dMeta:     dbIndex.Meta,
			small:     dbIndex.Index != nil,
			partition: partition,
			dataIndex: dbIndex.Index,
		}
		obj.key = generateObjectKey(d.hashPrefix, d.hashSuffix, obj.name, obj.partition)

		// 2 race conditions which should be handled carefully
		// could arise from here:
		// 1. the object has been deleted
		// 2. the object has been override
		// We we detail how to handle the race in each possible race point.

		// POINT #1:
		// For LO:
		//   race 1 would cause error
		//	 race 2 would cause checksum mismatch
		// For SO:
		//   race 1 would cause checksum mismatch
		//	 race 2 would cause checksum mismatch

		reader, err := d.NewReader(obj)

		// Race 1 in POINT #1 for LO would be detectd here. Since the FileNotExists
		// would be raised,  thus it is safe to ignore the object.
		// Technically, this is not an error, but it should be ok to consider
		// it as an error here. That is why we don't distinguish the errors.
		if err != nil {
			stat.Errors++
			glogger.Error("unable to create object reader",
				zap.String("object", obj.name),
				zap.String("partition", obj.partition),
				zap.Error(err))
			continue
		}

		// POINT #2:
		// For LO:
		//   race 1 would NOT cause checksum mismatch
		//	 race 2 would NOT cause checksum mismatch
		// For SO:
		//   race 1 would cause checksum mismatch
		//	 race 2 would cause checksum mismatch
		hash := md5.New()

		// We use a 64K buffer
		buf := make([]byte, 64*1024)
		for {

			nr, er := reader.Read(buf)
			if nr > 0 {
				nw, ew := hash.Write(buf[0:nr])
				if ew != nil {
					err = ew
					break
				}

				if nr != nw {
					err = io.ErrShortWrite
					break
				}
			}

			if er != nil {
				if er != io.EOF {
					err = er
				}
				break
			}

			bytesQuota = d.limitAuditRate(bytesQuota, gconf.AuditorBPS, int64(nr),
				LIMITER_BUFFER)
		}

		// Neither race cause error here, thus it is reasonable
		// to stop the whole audition process here since unexpected
		// IO error occurs..
		if err != nil {
			stat.Errors++
			glogger.Error("unable to read object data, stop auditing",
				zap.String("object", obj.name),
				zap.String("partition", obj.partition),
				zap.Error(err))
			break
		}
		checksum := fmt.Sprintf("%x", hash.Sum(nil))

		if checksum != obj.meta.SystemMeta[common.HEtag] {
			glogger.Info("unexpected checksum detected",
				zap.String("object", obj.name),
				zap.String("checksum", checksum))

			// Double check if the mismatch is caused by race
			canary := &PackObject{
				key: obj.key,
			}
			if err := d.LoadObjectMeta(canary); err != nil {
				glogger.Error("unable to load meta of object",
					zap.String("object", obj.name), zap.Error(err))
				continue
			}

			if canary.meta.Timestamp != obj.meta.Timestamp {
				glogger.Info("object has been modified",
					zap.String("object", obj.name),
					zap.String("origin-timestamp", obj.meta.Timestamp),
					zap.String("timestamp", canary.meta.Timestamp))
				continue
			}

			// canary has more detail than the origin one
			if err := d.QuarantineObject(canary); err != nil {
				glogger.Error("unable to quarantine object, stop auditing",
					zap.String("object", obj.name), zap.Error(err))
				break
			}
			stat.Quarantines++
		}

		stat.ProcessedFiles++
		stat.ProcessedBytes += obj.meta.DataSize
	}

	return stat, nil

}

func (d *PackDevice) saveQurantinedDBIndex(dir string, partType PartType, dbIndex *DBIndex) error {
	b, err := json.Marshal(dbIndex)
	if err != nil {
		glogger.Error("unable to jsonify db index during object quaranting",
			zap.String("object", dbIndex.Meta.Name),
			zap.Error(err))
		return err
	}

	p := filepath.Join(dir, fmt.Sprintf("%s.json", partType))

	f, err := os.Create(p)
	if err != nil {
		glogger.Error("unable to create json file to save db index",
			zap.String("path", p),
			zap.Error(err))
		return err
	}
	defer f.Close()

	_, err = f.Write(b)

	return err
}

func (d *PackDevice) saveQurantinedObject(obj *PackObject) error {
	// Do nothing if the object is a SO
	if obj.small {
		return nil
	}
	destDir := filepath.Join(QuarantineDir(d.driveRoot, d.device, d.policy), obj.key)
	// os.Rename don't allow existing dest dir
	if err := os.MkdirAll(filepath.Dir(destDir), 0755); err != nil {
		glogger.Error("unable to create quarantine dir",
			zap.String("path", filepath.Dir(destDir)),
			zap.Error(err))
		return err
	}

	hashDir := filepath.Join(d.objectsDir, obj.key)
	err := os.Rename(hashDir, destDir)
	if err != nil {
		glogger.Error("unable to move object to quarantine dir",
			zap.String("path", hashDir),
			zap.Error(err))
	}
	return err
}

func (d *PackDevice) saveQurantinedObjectContext(obj *PackObject) error {
	destDir := filepath.Join(QuarantineDir(d.driveRoot, d.device, d.policy), obj.key)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		glogger.Error("unable to create quarantine dir",
			zap.String("object", obj.meta.Name),
			zap.String("path", destDir),
			zap.Error(err))
		return err
	}

	if err := d.saveQurantinedDBIndex(
		destDir, DATA, &DBIndex{obj.dataIndex, obj.dMeta}); err != nil {
		glogger.Error("unable to save db index to file",
			zap.String("object", obj.meta.Name),
			zap.String("object-key", obj.key),
			zap.String("part-type", "data"),
			zap.Error(err))
		return err
	}

	if obj.metaIndex == nil {
		return nil
	}

	err := d.saveQurantinedDBIndex(
		destDir, META, &DBIndex{obj.metaIndex, obj.mMeta})
	if err != nil {
		glogger.Error("unable to save db index to file",
			zap.String("object", obj.meta.Name),
			zap.String("object-key", obj.key),
			zap.String("part-type", "meta"),
			zap.Error(err))
	}

	return err
}

func (d *PackDevice) QuarantineObject(obj *PackObject) error {
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

	// Prevent the corrupted object from being read first
	if err := d.clearDBIndexes(obj); err != nil {
		glogger.Error("unable to clear db index for quarantined object",
			zap.String("object", obj.name),
			zap.String("object-key", obj.key),
			zap.Error(err))
		return err
	}
	go InvalidateHash(filepath.Join(d.objectsDir, obj.key))

	if err := d.saveQurantinedObject(obj); err != nil {
		glogger.Error("unable to save object",
			zap.String("object", obj.name),
			zap.String("object-key", obj.key),
			zap.Error(err))
		return err
	}

	if err := d.saveQurantinedObjectContext(obj); err != nil {
		glogger.Error("unable to save object context",
			zap.String("object", obj.name),
			zap.String("object-key", obj.key),
			zap.Error(err))
		return err
	}

	return nil
}
