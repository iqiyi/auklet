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

package swift

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/iqiyi/auklet/common/fs"
	"github.com/iqiyi/auklet/common/pickle"

	"go.uber.org/zap"
)

func TempDir(driveRoot string, device string) string {
	return filepath.Join(driveRoot, device, "tmp")
}

func IsFileNotExist(path string) bool {
	_, err := os.Stat(path)

	return os.IsNotExist(err)
}

func ConsolidateHashes(pklPath, invalidPath string) (map[string]string, error) {
	var err error
	partitionDir := path.Dir(pklPath)
	deviceDir := path.Dir(partitionDir)

	if IsFileNotExist(pklPath) {
		// no hashes at all -> everything's invalid, so empty the file with
		// the invalid suffixes in it, if it exists
		if !IsFileNotExist(invalidPath) {
			if err = os.Truncate(invalidPath, 0); err != nil {
				glogger.Error(fmt.Sprintf("cannot truncate invalidate hash file %s", invalidPath))
			}
		}

		return nil, err
	}

	pLock, err := fs.LockPath(path.Dir(pklPath), time.Second*10)
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
		err = SaveHashesPkl(hashes, pklPath, path.Join(deviceDir, "tmp"), partitionDir)
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

func GetFileMTime(filePath string) (int64, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}

	return info.ModTime().Unix(), nil
}

// Load hash list from hashes.pkl
// TODO: need to remove corrupted hashes.pkl file
func LoadPklHashes(pklPath string) (map[string]string, error) {
	data, err := ioutil.ReadFile(pklPath)
	if err != nil {
		glogger.Error(fmt.Sprintf("cannot read content of hashes pkl file: %s", pklPath))

		return nil, err
	}

	v, err := pickle.PickleLoads(data)
	if err != nil {
		glogger.Error(fmt.Sprintf("cannot deserialize pickle file: %s", pklPath))
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

func SaveHashesPkl(hashes map[string]string, pklPath, tempDir, partitionDir string) error {
	tFile, err := fs.NewAtomicFileWriter(tempDir, partitionDir)
	if err != nil {
		glogger.Error(fmt.Sprintf("cannot create temp file in %s for partition %s", tempDir, partitionDir))
		return err
	}
	defer tFile.Abandon()

	_, err = tFile.Write(pickle.PickleDumps(hashes))
	if err != nil {
		glogger.Error(fmt.Sprintf("cannot flush hashes data to temp file in %s for partition %s", tempDir, partitionDir))
		return err
	}

	err = tFile.Save(pklPath)
	if err != nil {
		glogger.Error(fmt.Sprintf("cannot save temp file in %s to partition %s", tempDir, partitionDir))
	}

	return err
}
