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
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefreshBundleHeader(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	partition := strconv.Itoa(rand.Int())

	b, err := OpenBundle(dir, partition)
	require.Nil(t, err)
	defer b.Cleanup()

	require.Equal(t, BundleVersion1, b.Version)
	require.True(t, SuperBlockSize < NeedleAlignment)
	require.Equal(t, int64(NeedleAlignment), b.BundleSize())
}

func TestFsBlockSize(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	partition := strconv.Itoa(rand.Int())

	b, err := OpenBundle(dir, partition)
	require.Nil(t, err)
	defer b.Cleanup()

	s, err := GetFsBlockSize(b.File)
	require.Nil(t, err)
	require.Equal(t, int64(NeedleAlignment), s)
}

func TestPunchSmallHole(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	partition := strconv.Itoa(rand.Int())
	b, err := OpenBundle(dir, partition)
	defer b.Cleanup()
	require.Nil(t, err)

	blkCnt, err := GetRealFsBlocks(b.File)
	require.Nil(t, err)
	require.Equal(t, int64(1), blkCnt)

	blkSize, err := GetFsBlockSize(b.File)
	require.Nil(t, err)

	dataBlks := 10
	data := make([]byte, int(blkSize)*dataBlks)
	rand.Read(data)

	_, err = b.Seek(0, 2)
	require.Nil(t, err)
	_, err = b.Write(data)
	require.Nil(t, err)

	require.Nil(t, b.PunchHole(blkSize, blkSize*2))
	blkCnt, err = GetRealFsBlocks(b.File)
	require.Nil(t, err)
	// a bundle header occupies a block
	require.Equal(t, int64(dataBlks-2+1), blkCnt)
}

// XFS applies an optimization technique called speculative preallocation
// to reduce the fragmentations. In a word, it will reserve more blocks for
// the file during writing. The extra blocks will be reclaimed later, typicall
// when the file is closed. But it is not guaranteed.
// The problem with this case is that XFS will reserved double size blocks for
// the file. For example, if a 8M file is created, 16M space will be reserved.
// Thus even a 8M hole is punched, the file will still occupy 8M space. This
// makes the test behaviour undetermined.
// One workaround to pass the test is mount the XFS file system with option
// -o allocsize=64k
// See http://xfs.org/index.php/XFS_FAQ#Q:_What_is_speculative_preallocation.3F
// and https://serverfault.com/questions/406069/why-are-my-xfs-filesystems-suddenly-consuming-more-space-and-full-of-sparse-file
func TestPunchBigHoleInXfs(t *testing.T) {
	// It is impossible to mount an file system with customized options in docker
	// container at the moment. Thus this case will be disabled effectively in
	// in order to make CI happy.
	xfsTmpDir := os.Getenv("UT_XFS_TMP_DIR")
	if xfsTmpDir == "" {
		return
	}
	dir, err := ioutil.TempDir(xfsTmpDir, "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	partition := strconv.Itoa(rand.Int())
	b, err := OpenBundle(dir, partition)
	defer b.Cleanup()
	require.Nil(t, err)

	blkCnt, err := GetRealFsBlocks(b.File)
	require.Nil(t, err)
	require.Equal(t, int64(1), blkCnt)

	blkSize, err := GetFsBlockSize(b.File)
	require.Nil(t, err)

	// Write 8M data
	dataBlks := 1024 * 2
	data := make([]byte, int(blkSize)*dataBlks)
	rand.Read(data)

	_, err = b.Seek(0, 2)
	require.Nil(t, err)

	_, err = b.Write(data)
	require.Nil(t, err)

	require.Nil(t, b.PunchHole(blkSize, blkSize*int64(dataBlks)))
	blkCnt, err = GetRealFsBlocks(b.File)
	require.Nil(t, err)
	// It is impossible to get the exact block count due to xfs's
	// delayed allocation.
	// See http://xfs.org/docs/xfsdocs-xml-dev/XFS_User_Guide/tmp/en-US/html/ch02s08.html
	// We can only test the block count approximately.
	// Here we ensure that about 95% space will be released.
	require.True(t, float64(blkCnt) < float64(dataBlks)*0.05)
}

// Seems that the allocation behaviour in ext4 is determined.
// This case run as expected in an ext4 file system.
func TestPunchBigHoleInExt4(t *testing.T) {
	// It is impossible to mount an ext4 file system in docker container
	// at the moment. Thus this case will be disabled effectively in in order
	// to make CI happy.
	ext4TmpDir := os.Getenv("UT_EXT4_TMP_DIR")
	if ext4TmpDir == "" {
		return
	}
	dir, err := ioutil.TempDir(ext4TmpDir, "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	partition := strconv.Itoa(rand.Int())
	b, err := OpenBundle(dir, partition)
	defer b.Cleanup()
	require.Nil(t, err)

	blkCnt, err := GetRealFsBlocks(b.File)
	require.Nil(t, err)
	require.Equal(t, int64(1), blkCnt)

	blkSize, err := GetFsBlockSize(b.File)
	require.Nil(t, err)

	dataBlks := 1024
	data := make([]byte, int(blkSize)*dataBlks)
	rand.Read(data)

	_, err = b.Seek(0, 2)
	require.Nil(t, err)

	_, err = b.Write(data)
	require.Nil(t, err)

	require.Nil(t, b.PunchHole(blkSize, blkSize*int64(dataBlks)))
	blkCnt, err = GetRealFsBlocks(b.File)
	require.Nil(t, err)
	// Seems it is possible to get the exact block count in ext3
	require.Equal(t, int64(1), blkCnt)
}
