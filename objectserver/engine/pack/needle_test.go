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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNeedleHeaderSerialization(t *testing.T) {
	n := &NeedleHeader{
		MagicNumber: NeedleMagicNumber,
		NeedleSize:  8096,
		MetaOffset:  4096 + 40,
		MetaSize:    255,
		DataOffset:  4096 + 40 + 256,
		DataSize:    5190,
	}

	b := make([]byte, NeedleHeaderSize)

	n.WriteToBuffer(b)

	n2 := &NeedleHeader{}
	n2.DeserializeFrom(b)

	require.Equal(t, n.MagicNumber, n2.MagicNumber)
	require.Equal(t, n.NeedleSize, n2.NeedleSize)
	require.Equal(t, n.MetaOffset, n2.MetaOffset)
	require.Equal(t, n.MetaSize, n2.MetaSize)
	require.Equal(t, n.DataOffset, n2.DataOffset)
	require.Equal(t, n.DataSize, n2.DataSize)
}

// Every needle will occupy 4K aligned disk space
func TestNeedleDiskSize(t *testing.T) {
	require.Equal(t, int64(0), CalculateDiskSize(40, 83, 440)%NeedleAlignment)
}

func TestBufferSize(t *testing.T) {
	require.Equal(t,
		int64(NeedleAlignment), CalculateBufferSize(NeedleHeaderSize, 83))
}

func TestBufferSize2(t *testing.T) {
	require.Equal(t,
		int64(DefaultDataBufferSize+NeedleAlignment), CalculateBufferSize(NeedleHeaderSize, -1))
}
