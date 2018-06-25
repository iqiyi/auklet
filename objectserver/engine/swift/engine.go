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

package swift

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
	"github.com/iqiyi/auklet/common/fs"
	"github.com/iqiyi/auklet/objectserver/engine"

	"go.uber.org/zap"
)

// SwiftObject implements an Object that is
// compatible with Swift's object server.
type SwiftObject struct {
	file         *os.File
	afw          fs.AtomicFileWriter
	hashDir      string
	tempDir      string
	dataFile     string
	metaFile     string
	workingClass string
	metadata     map[string]string
	reserve      int64
	reclaimAge   int64
	asyncWG      *sync.WaitGroup // Used to keep track of async goroutines
}

// Metadata returns the object's metadata.
func (o *SwiftObject) Metadata() map[string]string {
	return o.metadata
}

// ContentLength parses and returns the Content-Length for the object.
func (o *SwiftObject) ContentLength() int64 {
	contentLength, err := strconv.ParseInt(
		o.metadata[common.HContentLength], 10, 64)
	if err != nil {
		return -1
	}
	return contentLength
}

// Quarantine removes the object's underlying files to
// the Quarantined directory on the device.
func (o *SwiftObject) Quarantine() error {
	o.Close()
	if QuarantineHash(o.hashDir) == nil {
		return InvalidateHash(o.hashDir)
	}
	return nil
}

// Exists returns true if the object exists, that is if it has a .data file.
func (o *SwiftObject) Exists() bool {
	return strings.HasSuffix(o.dataFile, ".data")
}

// Copy copies all data from the underlying .data file to the given writers.
func (o *SwiftObject) Copy(dsts ...io.Writer) (written int64, err error) {
	if len(dsts) == 1 {
		return io.Copy(dsts[0], o.file)
	}

	return common.Copy(o.file, dsts...)
}

// CopyRange copies data in the range of start to end from the underlying
// .data file to the writer.
func (o *SwiftObject) CopyRange(
	w io.Writer, start int64, end int64) (int64, error) {
	if _, err := o.file.Seek(start, os.SEEK_SET); err != nil {
		return 0, err
	}
	return common.CopyN(o.file, end-start, w)
}

// Repr returns a string that identifies the object in some useful way,
// used for logging.
func (o *SwiftObject) Repr() string {
	if o.dataFile != "" && o.metaFile != "" {
		return fmt.Sprintf("SwiftObject(%s, %s)", o.dataFile, o.metaFile)
	}

	if o.dataFile != "" {
		return fmt.Sprintf("SwiftObject(%s)", o.dataFile)
	}

	return fmt.Sprintf("SwiftObject(%s)", o.hashDir)
}

func (o *SwiftObject) newFile(class string, size int64) (io.Writer, error) {
	var err error
	o.Close()
	if o.afw, err = fs.NewAtomicFileWriter(o.tempDir, o.hashDir); err != nil {
		return nil, ErrTempFileNotCreated
	}

	if err := o.afw.Preallocate(size, o.reserve); err != nil {
		o.afw.Abandon()
		return nil, ErrDriveFull
	}
	o.workingClass = class
	return o.afw, nil
}

// SetData is called to set the object's data.
// It takes a size (if available, otherwise set to zero).
func (o *SwiftObject) SetData(size int64) (io.Writer, error) {
	return o.newFile("data", size)
}

// Commit commits an open data file to disk, given the metadata.
func (o *SwiftObject) Commit(metadata map[string]string) error {
	defer o.afw.Abandon()
	timestamp, ok := metadata[common.XTimestamp]
	if !ok {
		return ErrTimestampNotFoundInMeta
	}
	if err := WriteMetadata(o.afw.Fd(), metadata); err != nil {
		return fmt.Errorf("Error writing metadata: %v", err)
	}
	fileName := filepath.Join(o.hashDir, fmt.Sprintf("%s.%s", timestamp, o.workingClass))
	o.afw.Save(fileName)
	o.asyncWG.Add(1)
	go func() {
		defer o.asyncWG.Done()
		HashCleanupListDir(o.hashDir, o.reclaimAge)
		if dir, err := os.OpenFile(o.hashDir, os.O_RDONLY, 0666); err == nil {
			dir.Sync()
			dir.Close()
		}
		InvalidateHash(o.hashDir)
	}()
	return nil
}

func (o *SwiftObject) CommitMeta(metadata map[string]string) error {
	if _, err := o.newFile("meta", 0); err != nil {
		return err
	}
	defer o.Close()
	return o.Commit(metadata)
}

// Delete deletes the object.
func (o *SwiftObject) Delete(metadata map[string]string) error {
	if _, err := o.newFile("ts", 0); err != nil {
		return err
	} else {
		defer o.Close()
		return o.Commit(metadata)
	}
}

// Close releases any resources used by the instance of SwiftObject
func (o *SwiftObject) Close() error {
	if o.afw != nil {
		defer o.afw.Abandon()
		o.afw = nil
	}
	if o.file != nil {
		defer o.file.Close()
		o.file = nil
	}
	return nil
}

type SwiftEngine struct {
	driveRoot      string
	hashPathPrefix string
	hashPathSuffix string
	reserve        int64
	reclaimAge     int64
	policy         int
	asyncWG        *sync.WaitGroup
}

func (f *SwiftEngine) objHashDir(vars map[string]string) string {
	h := md5.New()
	text := fmt.Sprintf("%s/%s/%s/%s%s", f.hashPathPrefix,
		vars["account"], vars["container"], vars["obj"], f.hashPathSuffix)
	io.WriteString(h, text)
	hexHash := hex.EncodeToString(h.Sum(nil))
	suffix := hexHash[29:32]
	return filepath.Join(f.driveRoot, vars["device"], PolicyDir(f.policy), vars["partition"], suffix, hexHash)
}

// New returns an instance of SwiftObject with the given parameters.
// Metadata is read in and if needData is true, the file is opened.
// AsyncWG is a waitgroup if the object spawns any async operations
func (f *SwiftEngine) New(vars map[string]string,
	needData bool) (engine.Object, error) {
	var err error
	sor := &SwiftObject{
		reclaimAge: f.reclaimAge,
		reserve:    f.reserve,
		asyncWG:    f.asyncWG,
	}
	sor.hashDir = f.objHashDir(vars)
	sor.tempDir = fs.TempDir(f.driveRoot, vars["device"], f.policy)
	sor.dataFile, sor.metaFile = ObjectFiles(sor.hashDir)
	if sor.Exists() {
		var stat os.FileInfo
		if needData {
			if sor.file, err = os.Open(sor.dataFile); err != nil {
				return nil, err
			}
			sor.metadata, err = OpenObjectMetadata(sor.file.Fd(), sor.metaFile)
			if err != nil {
				sor.Quarantine()
				return nil, ErrMetaCorruption
			}
		} else {
			sor.metadata, err = ObjectMetadata(sor.dataFile, sor.metaFile)
			if err != nil {
				sor.Quarantine()
				return nil, ErrMetaCorruption
			}
		}
		if sor.file != nil {
			if stat, err = sor.file.Stat(); err != nil {
				sor.Close()
				return nil, ErrFileCorruption
			}
		} else if stat, err = os.Stat(sor.dataFile); err != nil {
			return nil, ErrFileCorruption
		}

		cl, err := strconv.ParseInt(sor.metadata[common.HContentLength], 10, 64)
		if err != nil {
			sor.Quarantine()
			return nil, ErrMetaCorruption
		}

		if stat.Size() != cl {
			sor.Quarantine()
			return nil, ErrFileCorruption
		}
	}

	return sor, nil
}

func (f *SwiftEngine) hashes(
	device string,
	partition string,
	recalculate []string,
	lsSuffixes bool,
	reclaimAge int64) (hashed int64, hashes map[string]string, err error) {

	defer func() {
		if err != nil {
			hashes = nil
		}
	}()

	partitionDir := filepath.Join(f.driveRoot, device, PolicyDir(f.policy), partition)
	pklPath := filepath.Join(partitionDir, HASH_FILE)
	invalidPath := filepath.Join(partitionDir, HASH_INVALIDATIONS_FILE)

	if IsFileNotExist(partitionDir) {
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

	mtime, err := GetFileMTime(pklPath)

	if err != nil && !os.IsNotExist(err) {
		glogger.Error("unable to get modification time of hashes pkl file",
			zap.String("path", pklPath), zap.Error(err))
		return
	}

	if lsSuffixes {
		suffs, _ := fs.ReadDirNames(partitionDir)
		hashes = make(map[string]string)

		for _, suff := range suffs {
			if len(suff) == 3 && hashes[suff] == "" {
				hashes[suff] = ""
			}
		}
	}

	for _, suff := range recalculate {
		hashes[suff] = ""
	}

	for suffix, hash := range hashes {
		if hash != "" {
			continue
		}

		modified = true
		suffixDir := filepath.Join(partitionDir, suffix)
		h, err := RecalculateSuffixHash(suffixDir, reclaimAge)
		switch err {
		case nil:
			hashes[suffix] = h
			hashed++
		case ErrPathNotDir:
			delete(hashes, suffix)
		default:
			glogger.Error("unable to hashing suffix",
				zap.String("partition", partition),
				zap.String("suffix", suffix), zap.Error(err))
		}
	}

	// Reset ignorable error
	err = nil

	if modified {
		// Use anonymous function so that the logic of lock release is clear.
		// If done is true or any unexpected error occurs, then GetHashes should
		// NOT be called recursively.
		var done bool
		done, err = func() (bool, error) {
			pLock, e := fs.LockPath(partitionDir, time.Second*10)
			defer pLock.Close()
			// Unexpected error occurs, no need to call GetHashes recursively
			if e != nil {
				return true, e
			}

			mt, e := GetFileMTime(pklPath)
			if !(forceRewrite || os.IsNotExist(e) || mtime == mt) {
				// If none of the conditions is met, then the hashes.pkl file shall
				// not be refreshed at the moment.
				// 1. A force rewrite is required
				// 2. hashes.pkl does not exist
				// 3. hashes.pkl has not been modified
				return false, e
			}

			if e = SaveHashesPkl(hashes, pklPath, TempDir(f.driveRoot, device), partitionDir); e != nil {
				glogger.Error("unable to rewrite hashes.pkl files",
					zap.String("partition", partition), zap.Error(err))
			}

			// If the SaveHashesPkl is successful, every thing works as expected.
			// Else unexpected error occurs.
			// So no matter pkl file gets refreshed successfully or not, we shall not
			// call GetHashes again.
			return true, e
		}()

		if !done {
			return f.hashes(device, partition, recalculate, lsSuffixes, reclaimAge)
		}
	}

	return
}

func (f *SwiftEngine) GetHashes(
	device, partition string, recalculate []string) (map[string]string, error) {
	_, hashes, err := f.hashes(device, partition, recalculate, false, ONE_WEEK)
	return hashes, err
}

func (f *SwiftEngine) Close() error {
	return nil
}

// creates a SwiftEngine given the object server configs.
func SwiftEngineConstructor(config conf.Config, policy *conf.Policy,
	flags *flag.FlagSet, wg *sync.WaitGroup) (engine.ObjectEngine, error) {

	var err error
	glogger, err = common.GetLogger(
		flags.Lookup("l").Value.(flag.Getter).Get().(string), "swift")
	if err != nil {
		common.BootstrapLogger.Printf("unable to config zap log: %v", err)
		os.Exit(1)
	}

	driveRoot := config.GetDefault("app:object-server", "devices", "/srv/node")
	reserve := config.GetInt("app:object-server", "fallocate_reserve", 0)
	hashPathPrefix, hashPathSuffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, ErrConfigNotLoaded
	}
	reclaimAge := config.GetInt(
		"app:object-server", "reclaim_age", int64(common.ONE_WEEK))

	return &SwiftEngine{
		driveRoot:      driveRoot,
		hashPathPrefix: hashPathPrefix,
		hashPathSuffix: hashPathSuffix,
		reserve:        reserve,
		reclaimAge:     reclaimAge,
		policy:         policy.Index,
		asyncWG:        wg,
	}, nil
}

func init() {
	engine.RegisterObjectEngine("replication", SwiftEngineConstructor)
}
