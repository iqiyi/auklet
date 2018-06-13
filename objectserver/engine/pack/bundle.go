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
	"syscall"

	"go.uber.org/zap"
)

type BundleVersion uint8

const (
	BundleVersion1       = BundleVersion(1)
	CurrentBundleVersion = BundleVersion1
)

const (
	SuperBlockSize     = 64
	SuperBlockDiskSize = NeedleAlignment
	BundleFileMode     = 0644
)

const (
	FALLOC_FL_KEEP_SIZE  = 0x1
	FALLOC_FL_PUNCH_HOLE = 0x2
)

/*
* Currently, A superblock only require 1 byte to track the bundle format
* version. However, since every needle is 4K aligned in the bundle file,
* the first 4K of every bundle will be dedicated to superblock.
* This means there a lot of space "reserved" for superblock and should be
* sufficient for later extending.
 */
type SuperBlock struct {
	Version BundleVersion
}

func (s *SuperBlock) Bytes() []byte {
	header := make([]byte, SuperBlockSize)
	header[0] = byte(s.Version)

	return header
}

func NewSuperBlock(header []byte) *SuperBlock {
	sb := &SuperBlock{
		Version: BundleVersion(header[0]),
	}

	return sb
}

type Bundle struct {
	*SuperBlock
	*os.File
	sync.Mutex

	partition string
}

func (b *Bundle) FlushSuperBlock() error {
	b.Lock()
	defer b.Unlock()

	if _, err := b.Seek(0, 0); err != nil {
		glogger.Error("unable to seek to begin of bundle",
			zap.String("partition", b.partition), zap.Error(err))
		return err
	}

	if _, err := b.Write(b.Bytes()); err != nil {
		glogger.Error("unable to flush bundle superblock",
			zap.String("partition", b.partition), zap.Error(err))
		return err
	}

	return nil
}

func (b *Bundle) PunchHole(offset, len int64) error {
	return syscall.Fallocate(
		int(b.Fd()), FALLOC_FL_KEEP_SIZE|FALLOC_FL_PUNCH_HOLE, offset, len)
}

func (b *Bundle) BundleSize() int64 {
	info, err := b.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

func (b *Bundle) Cleanup() error {
	if err := b.FlushSuperBlock(); err != nil {
		return err
	}

	if err := b.Close(); err != nil {
		glogger.Error("unable to close bundle",
			zap.String("partition", b.partition), zap.Error(err))
		return err
	}

	return nil
}

func formatBundleFile(bundlePath string) error {
	st, err := os.Stat(bundlePath)
	if err != nil && !os.IsNotExist(err) {
		glogger.Error("unable to stat bundle",
			zap.String("bundle-path", bundlePath), zap.Error(err))
		return err
	}

	if !(os.IsNotExist(err) || st.Size() == 0) {
		return nil
	}

	vf, err := os.OpenFile(bundlePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		glogger.Error("unable to create bundle",
			zap.String("bundle-path", bundlePath), zap.Error(err))
		return err
	}
	defer vf.Close()

	sb := &SuperBlock{
		Version: CurrentBundleVersion,
	}

	b := sb.Bytes()
	if _, err = vf.Write(b); err != nil {
		glogger.Error("unable to write bundle header",
			zap.String("bundle-path", bundlePath), zap.Error(err))
		return err
	}

	if _, err = vf.Write(padding[0:(NeedleAlignment - len(b))]); err != nil {
		glogger.Error("unable to pad bundle header",
			zap.String("bundle-path", bundlePath), zap.Error(err))
		return err
	}

	return nil
}

func OpenBundle(devPath, partition string) (*Bundle, error) {
	vpd := filepath.Join(devPath, partition)
	if err := os.MkdirAll(vpd, 0755); err != nil {
		glogger.Error("unable to create bundle directory",
			zap.String("bundle-dir", vpd), zap.Error(err))
		return nil, err
	}

	vp := filepath.Join(vpd, "bundle.data")

	if err := formatBundleFile(vp); err != nil {
		return nil, err
	}

	vf, err := os.OpenFile(vp, os.O_RDWR|os.O_SYNC, BundleFileMode)
	if err != nil {
		glogger.Error("unable to open bundle",
			zap.String("bundle-file", vp), zap.Error(err))
		return nil, err
	}

	if _, err = vf.Seek(0, 0); err != nil {
		glogger.Error("unable to seek to begin of bundle",
			zap.String("bundle-file", vp), zap.Error(err))
		return nil, err
	}

	header := make([]byte, SuperBlockSize)
	if _, err = vf.Read(header); err != nil {
		glogger.Error("cannot read bundle super block",
			zap.String("bundle-file", vp), zap.Error(err))
		return nil, err
	}
	sb := NewSuperBlock(header)

	return &Bundle{
		sb,
		vf,
		sync.Mutex{},
		partition,
	}, nil
}
