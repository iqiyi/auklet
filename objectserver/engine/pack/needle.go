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
	"encoding/binary"
)

const (
	NeedleMagicNumber     = 0xdeadbeef
	NeedleAlignment       = 4096
	NeedleHeaderSize      = 40
	DefaultDataBufferSize = 1024 * 256
	DefaultMetaBufferSize = 512
)

var padding = make([]byte, NeedleAlignment)

type NeedleHeader struct {
	MagicNumber uint32
	NeedleSize  int64
	MetaOffset  int64
	MetaSize    int32
	DataOffset  int64
	DataSize    int64
}

func (n *NeedleHeader) WriteToBuffer(b []byte) {
	binary.LittleEndian.PutUint32(b[0:4], n.MagicNumber)
	binary.LittleEndian.PutUint64(b[4:12], uint64(n.NeedleSize))
	binary.LittleEndian.PutUint64(b[12:20], uint64(n.MetaOffset))
	binary.LittleEndian.PutUint32(b[20:24], uint32(n.MetaSize))
	binary.LittleEndian.PutUint64(b[24:32], uint64(n.DataOffset))
	binary.LittleEndian.PutUint64(b[32:40], uint64(n.DataSize))
}

func (n *NeedleHeader) DeserializeFrom(b []byte) {
	n.MagicNumber = binary.LittleEndian.Uint32(b[0:4])
	n.NeedleSize = int64(binary.LittleEndian.Uint64(b[4:12]))
	n.MetaOffset = int64(binary.LittleEndian.Uint64(b[12:20]))
	n.MetaSize = int32(binary.LittleEndian.Uint32(b[20:24]))
	n.DataOffset = int64(binary.LittleEndian.Uint64(b[24:32]))
	n.DataSize = int64(binary.LittleEndian.Uint64(b[32:40]))
}

// Calculate memory buffer size for SO
func CalculateBufferSize(headerSize int32, dataSize int64) int64 {
	if dataSize < 0 {
		dataSize = DefaultDataBufferSize
	}

	realSize := int64(headerSize+DefaultMetaBufferSize) + dataSize
	blkCnt := realSize / NeedleAlignment
	if realSize%NeedleAlignment != 0 {
		blkCnt += 1
	}

	return blkCnt * NeedleAlignment
}

func CalculateDiskSize(headerSize int32, dataSize int64, metaSize int32) int64 {
	realSize := int64(headerSize+metaSize) + dataSize
	blkCnt := realSize / NeedleAlignment
	if realSize%NeedleAlignment != 0 {
		blkCnt += 1
	}

	return blkCnt * NeedleAlignment
}
