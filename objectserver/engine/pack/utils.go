// Copyright (c) 2015 Rackspace
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
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/fs"
	"github.com/iqiyi/auklet/common/pickle"
)

const (
	NAME                    = "pack"
	ONE_WEEK                = 604800
	METADATA_CHUNK_SIZE     = 65536
	HASH_FILE               = "hashes.pkl"
	HASH_INVALIDATIONS_FILE = "hashes.invalid"
)

func GetFsBlockSize(file *os.File) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Fstatfs(int(file.Fd()), &stat); err != nil {
		return 0, err
	}
	return stat.Bsize, nil
}

func GetRealFsBlocks(file *os.File) (int64, error) {
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(file.Fd()), &stat); err != nil {
		return 0, err
	}
	return stat.Blocks / 8, nil
}

func generateKeyFromHash(partition, hash string) string {
	return fmt.Sprintf("/%s/%s/%s", partition, hash[29:32], hash)
}

func generateObjectKey(prefix, suffix, name, partition string) string {
	h := md5.New()
	io.WriteString(h, fmt.Sprintf("%s%s%s", prefix, name, suffix))
	hexHash := hex.EncodeToString(h.Sum(nil))

	return generateKeyFromHash(partition, hexHash)
}

func splitObjectKey(key string) []string {
	f := func(c rune) bool {
		return c == '/'
	}

	return strings.FieldsFunc(key, f)
}

var immutableHeaders = map[string]bool{
	common.HContentType:   true,
	common.HContentLength: true,
	common.HEtag:          true,
	"Deleted":             true,
}

func isObjectSystemMeta(header string) bool {
	return immutableHeaders[header] || common.IsSysMeta("object", header)
}

func PackDevicePaths(device, driveRoot string, policy int) (string, string) {
	suffix := ""
	if policy != 0 {
		suffix = fmt.Sprintf("-%d", policy)
	}

	objPath := filepath.Join(driveRoot, device, fmt.Sprintf("objects%s", suffix))
	dbPath := filepath.Join(driveRoot, device, fmt.Sprintf("pack-meta%s", suffix))

	return objPath, dbPath
}

func QuarantineDir(driveRoot string, device string, policy int) string {
	suffix := ""
	if policy != 0 {
		suffix = fmt.Sprintf("-%d", policy)
	}

	return filepath.Join(driveRoot, device, "quarantined", fmt.Sprintf("objects%s", suffix))
}

// Load hash list from hashes.pkl
// TODO: need to remove corrupted hashes.pkl file
func LoadPklHashes(pklPath string) (map[string]string, error) {
	data, err := ioutil.ReadFile(pklPath)
	if err != nil {
		glogger.Error("cannot read content of hashes pkl file",
			zap.String("path", pklPath), zap.Error(err))
		return nil, err
	}

	v, err := pickle.PickleLoads(data)
	if err != nil {
		glogger.Error("cannot deserialize pickle file",
			zap.String("path", pklPath), zap.Error(err))
		return nil, err
	}

	pickledHashes, ok := v.(map[interface{}]interface{})
	if !ok {
		return nil, ErrMalformedPickleFile
	}
	hashes := make(map[string]string)

	for suff, hash := range pickledHashes {
		if hashes[suff.(string)], ok = hash.(string); !ok {
			hashes[suff.(string)] = ""
		}
	}

	return hashes, nil
}

// Load hash list from hashes.invalid
func LoadInvalidSuffixes(invalidPath string) ([]string, error) {
	ivf, err := os.OpenFile(invalidPath, os.O_RDWR, 0660)
	if err != nil {
		return nil, err
	}
	defer ivf.Close()

	scanner := bufio.NewScanner(ivf)
	suffixes := make([]string, 0)
	for scanner.Scan() {
		suff := scanner.Text()
		if len(suff) == 3 && strings.Trim(suff, "0123456789abcdef") == "" {
			suffixes = append(suffixes, suff)
		}
	}

	return suffixes, nil
}

func SaveHashesPkl(
	hashes map[string]string, pklPath, tempDir, partitionDir string) error {
	tFile, err := fs.NewAtomicFileWriter(tempDir, partitionDir)
	if err != nil {
		glogger.Error("unable to create temp file",
			zap.String("path", tempDir),
			zap.String("partition", partitionDir),
			zap.Error(err))
		return err
	}
	defer tFile.Abandon()

	_, err = tFile.Write(pickle.PickleDumps(hashes))
	if err != nil {
		glogger.Error("unable to flush hashes data to temp file",
			zap.String("path", tempDir),
			zap.String("partition", partitionDir),
			zap.Error(err))
		return err
	}

	err = tFile.Save(pklPath)
	if err != nil {
		glogger.Error("unable to rename temp file",
			zap.String("path", tempDir),
			zap.String("partition", partitionDir),
			zap.Error(err))
	}

	return err
}

func ConsolidateHashes(pklPath, invalidPath string) (map[string]string, error) {
	var err error
	partitionDir := filepath.Dir(pklPath)
	deviceDir := filepath.Dir(partitionDir)

	if fs.IsFileNotExist(pklPath) {
		// no hashes at all -> everything's invalid, so empty the file with
		// the invalid suffixes in it, if it exists
		if !fs.IsFileNotExist(invalidPath) {
			if err = os.Truncate(invalidPath, 0); err != nil {
				glogger.Error("unable to truncate invalidate hash files",
					zap.String("path", invalidPath), zap.Error(err))
			}
		}

		return nil, err
	}

	pLock, err := fs.LockPath(filepath.Dir(pklPath), time.Second*10)
	defer pLock.Close()
	if err != nil {
		return nil, ErrLockPath
	}

	hashes, err := LoadPklHashes(pklPath)
	if err != nil {
		glogger.Error("unable to load hashes pkl file",
			zap.String("path", pklPath), zap.Error(err))
		return nil, err
	}

	modified := false

	suffixes, err := LoadInvalidSuffixes(invalidPath)
	if err != nil && !os.IsNotExist(err) {
		glogger.Error("unable to load invalidate hashes file",
			zap.String("path", invalidPath), zap.Error(err))
		return nil, err
	}

	for _, suff := range suffixes {
		hashes[suff] = ""
		modified = true
	}

	if modified {
		err = SaveHashesPkl(hashes, pklPath, filepath.Join(deviceDir, "tmp"), partitionDir)
		if err != nil {
			glogger.Error(fmt.Sprintf("cannot refresh hashes file %s", pklPath))
			return nil, err
		}
	}

	if err = os.Truncate(invalidPath, 0); err != nil && !os.IsNotExist(err) {
		glogger.Error("unable to truncate consolidated invalidate hash file",
			zap.String("path", invalidPath), zap.Error(err))
		return nil, err
	}

	return hashes, nil
}

func RawReadMetadata(fileNameOrFd interface{}) ([]byte, error) {
	var pickledMetadata []byte
	offset := 0
	for index := 0; ; index += 1 {
		var metadataName string
		// get name of next xattr
		if index == 0 {
			metadataName = "user.swift.metadata"
		} else {
			metadataName = "user.swift.metadata" + strconv.Itoa(index)
		}
		// get size of xattr
		length, err := fs.Getxattr(fileNameOrFd, metadataName, nil)
		if err != nil || length <= 0 {
			break
		}
		// grow buffer to hold xattr
		for cap(pickledMetadata) < offset+length {
			pickledMetadata = append(pickledMetadata, 0)
		}
		pickledMetadata = pickledMetadata[0 : offset+length]
		if _, err := fs.Getxattr(fileNameOrFd, metadataName, pickledMetadata[offset:]); err != nil {
			return nil, err
		}
		offset += length
	}
	return pickledMetadata, nil
}

func ReadMetadata(fileNameOrFd interface{}) (map[string]string, error) {
	pickledMetadata, err := RawReadMetadata(fileNameOrFd)
	if err != nil {
		return nil, err
	}
	v, err := pickle.PickleLoads(pickledMetadata)
	if err != nil {
		return nil, err
	}
	if v, ok := v.(map[interface{}]interface{}); ok {
		metadata := make(map[string]string, len(v))
		for mk, mv := range v {
			var mks, mvs string
			if mks, ok = mk.(string); !ok {
				return nil, fmt.Errorf("Metadata key not string: %v", mk)
			} else if mvs, ok = mv.(string); !ok {
				return nil, fmt.Errorf("Metadata value not string: %v", mv)
			}
			metadata[mks] = mvs
		}
		return metadata, nil
	}
	return nil, fmt.Errorf("Unpickled metadata not correct type")
}

func RawWriteMetadata(fd uintptr, buf []byte) error {
	for index := 0; len(buf) > 0; index++ {
		var metadataName string
		if index == 0 {
			metadataName = "user.swift.metadata"
		} else {
			metadataName = "user.swift.metadata" + strconv.Itoa(index)
		}
		writelen := METADATA_CHUNK_SIZE
		if len(buf) < writelen {
			writelen = len(buf)
		}
		if _, err := fs.Setxattr(fd, metadataName, buf[0:writelen]); err != nil {
			return err
		}
		buf = buf[writelen:]
	}
	return nil
}

func WriteMetadata(fd uintptr, v map[string]string) error {
	return RawWriteMetadata(fd, pickle.PickleDumps(v))
}

func InvalidateHash(hashDir string) error {
	suffDir := filepath.Dir(hashDir)
	partitionDir := filepath.Dir(suffDir)

	partitionLock, err := fs.LockPath(partitionDir, 10*time.Second)
	if err != nil {
		return err
	}
	defer partitionLock.Close()
	fp, err := os.OpenFile(filepath.Join(partitionDir, HASH_INVALIDATIONS_FILE),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	defer fp.Close()
	_, err = fmt.Fprintf(fp, "%s\n", filepath.Base(suffDir))
	return err
}

func HashCleanupListDir(hashDir string, reclaimAge int64) ([]string, error) {
	fileList, err := fs.ReadDirNames(hashDir)
	returnList := []string{}
	if err != nil {
		if os.IsNotExist(err) {
			return returnList, nil
		}
		if fs.IsNotDir(err) {
			return returnList, ErrPathNotDir
		}
		return returnList, err
	}
	deleteRest := false
	deleteRestMeta := false
	if len(fileList) == 1 {
		filename := fileList[0]
		if strings.HasSuffix(filename, ".ts") {
			withoutSuffix := strings.Split(filename, ".")[0]
			if strings.Contains(withoutSuffix, "_") {
				withoutSuffix = strings.Split(withoutSuffix, "_")[0]
			}
			timestamp, _ := strconv.ParseFloat(withoutSuffix, 64)
			if time.Now().Unix()-int64(timestamp) > reclaimAge {
				os.RemoveAll(hashDir + "/" + filename)
				return returnList, nil
			}
		}
		returnList = append(returnList, filename)
	} else {
		for index := len(fileList) - 1; index >= 0; index-- {
			filename := fileList[index]
			if deleteRest {
				os.RemoveAll(hashDir + "/" + filename)
			} else {
				if strings.HasSuffix(filename, ".meta") {
					if deleteRestMeta {
						os.RemoveAll(hashDir + "/" + filename)
						continue
					}
					deleteRestMeta = true
				}
				if strings.HasSuffix(filename, ".ts") || strings.HasSuffix(filename, ".data") {
					// TODO: check .ts time for expiration
					deleteRest = true
				}
				returnList = append(returnList, filename)
			}
		}
	}
	return returnList, nil
}
