// Copyright (c) 2015 Rackspace
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

package pickle

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type mystring string

func BenchmarkUnmarshal(b *testing.B) {
	pickled := PickleDumps(map[string]string{
		"Content-Length": "65536", "Content-Type": "application/octet-stream", "ETag": "fcd6bcb56c1689fcef28b57c22475bad",
		"X-Timestamp": "1422766779.57463", "name": "/someaccountname/somecontainername/5821142269423797100"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var v map[string]string
		Unmarshal(pickled, &v)
	}
}

func TestUnmarshalStruct(t *testing.T) {
	type Teststruct struct {
		Tmap    map[mystring]int64
		Tstring string
		Tint    int64
		Tfloat  float64
		Tbool   bool
		Tslice  []int
	}
	ts := Teststruct{
		Tmap:    map[mystring]int64{"hello": 42},
		Tstring: "testing",
		Tint:    1000,
		Tfloat:  3.14,
		Tbool:   true,
		Tslice:  []int{1, 2, 3},
	}
	serialized := PickleDumps(ts)
	var rs Teststruct
	require.Nil(t, Unmarshal(serialized, &rs))
	require.Equal(t, ts, rs)
}

func TestUnmarshalSomeRecursion(t *testing.T) {
	type Teststruct struct {
		Depth int
		Next  *Teststruct
	}
	ts := Teststruct{
		Depth: 1,
		Next: &Teststruct{
			Depth: 2,
			Next: &Teststruct{
				Depth: 3,
				Next: &Teststruct{
					Depth: 4,
				},
			},
		},
	}
	serialized := PickleDumps(ts)
	var rs Teststruct
	require.Nil(t, Unmarshal(serialized, &rs))
	require.Equal(t, ts, rs)
	require.Nil(t, rs.Next.Next.Next.Next)
}

func TestUnmarshalMap(t *testing.T) {
	src := map[mystring]int64{
		mystring("hello"): 42,
		mystring("there"): 91,
		mystring("you"):   113,
	}
	var result map[string]int
	require.Nil(t, unpack(reflect.ValueOf(src), reflect.ValueOf(&result)))
	require.Equal(t, 3, len(result))
	require.Equal(t, 42, result["hello"])
	require.Equal(t, 91, result["there"])
	require.Equal(t, 113, result["you"])
}

func TestUnmarshalString(t *testing.T) {
	var result string
	require.Nil(t, unpack(reflect.ValueOf("1"), reflect.ValueOf(&result)))
	require.Equal(t, "1", result)
}

func TestUnmarshalInt(t *testing.T) {
	for _, src := range []interface{}{int64(1), int(1), float32(1), float64(1)} {
		var result int64
		require.Nil(t, unpack(reflect.ValueOf(src), reflect.ValueOf(&result)))
		require.Equal(t, int64(1), result)
	}
}

func TestUnmarshalFloat(t *testing.T) {
	for _, src := range []interface{}{int64(1), int(1), float32(1), float64(1)} {
		var result float64
		require.Nil(t, unpack(reflect.ValueOf(src), reflect.ValueOf(&result)))
		require.Equal(t, float64(1.0), result)
	}
}

func TestUnmarshalBool(t *testing.T) {
	for _, src := range []interface{}{int64(1), float32(1), true} {
		var result bool
		require.Nil(t, unpack(reflect.ValueOf(src), reflect.ValueOf(&result)))
		require.True(t, result, fmt.Sprintf("%s", src))
	}

	for _, src := range []interface{}{int64(0), float32(0), false} {
		var result bool
		require.Nil(t, unpack(reflect.ValueOf(src), reflect.ValueOf(&result)))
		require.False(t, result)
	}
}

func TestUnmarshalSlice(t *testing.T) {
	var result []int
	src := []int8{1, 2, 3}
	require.Nil(t, unpack(reflect.ValueOf(src), reflect.ValueOf(&result)))
	require.Equal(t, 3, len(result))
	require.Equal(t, 1, result[0])
	require.Equal(t, 2, result[1])
	require.Equal(t, 3, result[2])
}

func TestUnmarshalRingBuilder(t *testing.T) {
	// a real life ring builder from my saio
	b, err := hex.DecodeString("1f8b08084ffa285900036f626a6563742d312e6275696c64657200ed99e992db4410c775e" +
		"d11b3188723e60e37e65ab24e20dc094960c30ee20819c2a508614f59aedab5ddf2b195546d151f20145ff8cc1bf0043c046" +
		"fc32330f2a599ee56081fa9722625d9a356cfaffffd9f5a95f5a377046e436e4c5436ecf67be08935b999a9c17eb7950cc1d" +
		"f3defccfec9e371bb3b1ccca2e24e960c52088e60ad215ce1dd8a60bd113a9efe2f1ca5675c796ca77976fb941e3bb0f14b0" +
		"49be6e59f2338d6108e704227c8a37d33baa2a3efb292053afd969d5ecfdc5dcc4c19cc1c55e18e22b8c7ce6206d4f422c7a" +
		"dcbbe4e79af9dd24d3784a3f3dc67057a6955df7cbf1d1aa435e1ebd007ec693fad084f4f9f28a6c7b23248b2513ce81faa0" +
		"ceaa2226bf17e32d413f9ec417fa286f0602bc9b2e44665768487a47b218287a78aadc66aacc66aacc66aacc6ff7ba8fffc1" +
		"75dddba028fc84af120068fee3ac503dafcb1ad993f4a34db6a028f45f07823d5cf0f972338b958cd9b0ff393631c17d7cce" +
		"f1e73bd8832cf76b467c499f1c5a0b166942730d3ed783d94df26e43278ec5a0eb98712537d7845cc182ebf272821ce62aee" +
		"5084e27da593b0ecfdbea507e4e21be9bdc0ab8b7767ddcba9eb015e07a83dd45fd893f51cda8d7b04e361557154784fbe60" +
		"8ccc9f596ae823bcdf517ef425a1fee9d7de49de9a06c786f95bb11fba75c77aa357546f9bec3b979af7115507eac0c5741f" +
		"94ec2cea67d28e7e63d41f5b467a9f7e999df337857d84addd98ea1fdc0a4bc7e7cedd495941aeb545e3d7603ad89ef27ce4" +
		"d3d849d45dd8bfd4fd5e308391fd17ee38aa8df67cf0fff56bfc5903f3f3c317f1e78b221fce970e7c3fce41bc7c535f3bbc" +
		"b5c2fa2ccb31ded1a71667c3168ac19e50acc743b5e17e5b709b90c2ebb964feea1c4541f5e113386cbef0a4a88b3986bf98" +
		"2d38976d68ec3f3b63a949f5388ef26b702eead5d1fb7ae2b6c05b8de6077517fe24f5433ea35ac934dc555c511e1bef9027" +
		"372bda5abe04e73fdc5bb90d6877b671f7967fa281bde5be56ec4fe29d79d6a4d9d51beef706ede6b5c05941f2bc35550be9" +
		"3b0b3691fcab9794f503ded59ea7d7ae6f70cde15b65277b663683f3029af1f5f3b7525a5c63a95578fdd406be2fb8973530" +
		"f616751f762ff53f53842ce47b4dfb822ea775f35ac3bca3a6130e4cf034f295937de40749251aab27838d25fe0e9f00f4f5" +
		"60fbabdd9b5b43fce86f08c70e5667fa2b2fd7ed286678b1f2082b69a0ce1b9089e6f1c41a3216bf3df2346f91ba3415fe77" +
		"b21dc3b29839bfd9e82177596f543d5eda4237869f7dcdff31c5e77002fa71b723d539dfcf78d57745830bd773bbc5997553" +
		"3a50e7dd57ca9734aaee598c3d0f16470a04609ec4847ae6baa6e4b415306c3f60f3b705a6e4da3e2c3a437526d3823f255d" +
		"bf09a70c647f03a877d36bcbac07e43784bec376decb7d2ea12fb6d8d9d277d479f31f3bb26f37b84f99cc17c7ecadc84f7d" +
		"39c72a6c385f0a7ba06bdc8815e0aaf2f403f10fe12f4431b7437ad2c412f2ff5fd28fc8deabb67b20ac2fab1c11a4e594fc" +
		"327e942d14f85a7413fe3403f0fd305e815112c41bfb041afa6b525a8d4a0c5f279fa2f758598f79ac9fbd59cf26b83f29b2" +
		"9e519f8d650f4bbf0f7fa58c9addcbe712b4d7a1d6d8be8d7f972703d74027902bfa48bd5a0df4a21debbf1e75fd7e4569ca" +
		"97c3a9e6e81ef2348c6dbff000ce59aa65a1d0000")
	require.Nil(t, err)
	r, err := gzip.NewReader(bytes.NewBuffer(b))
	require.Nil(t, err)
	data, err := ioutil.ReadAll(r)
	require.Nil(t, err)
	// load the ring builder into an old-school interface{}
	unst1, err := PickleLoads(data)
	require.Nil(t, err)

	// round-trip the ring builder through a struct unmarshal/marshal
	type RingBuilderDevice struct {
		ReplicationPort int64   `pickle:"replication_port"`
		Meta            string  `pickle:"meta"`
		PartsWanted     int64   `pickle:"parts_wanted"`
		Device          string  `pickle:"device"`
		Zone            int64   `pickle:"zone"`
		Weight          float64 `pickle:"weight"`
		Ip              string  `pickle:"ip"`
		Region          int64   `pickle:"region"`
		Port            int64   `pickle:"port"`
		ReplicationIp   string  `pickle:"replication_ip"`
		Parts           int64   `pickle:"parts"`
		Id              int64   `pickle:"id"`
	}
	type RingBuilder struct {
		LastPartGatherStart int64               `pickle:"_last_part_gather_start"`
		LastPartMovesEpoch  int64               `pickle:"_last_part_moves_epoch"`
		PartPower           int64               `pickle:"part_power"`
		DevsChanged         bool                `pickle:"devs_changed"`
		Replicas            float64             `pickle:"replicas"`
		MinPartHours        int64               `pickle:"min_part_hours"`
		Parts               int64               `pickle:"parts"`
		Overload            float64             `pickle:"overload"`
		Dispersion          float64             `pickle:"dispersion"`
		Version             int64               `pickle:"version"`
		Devices             []RingBuilderDevice `pickle:"devs"`
		RemoveDevs          []interface{}       `pickle:"_remove_devs"`
		LastPartMoves       struct {
			ArrayType string // should be "B"
			Data      []uint8
		} `pickle:"_last_part_moves"`
		Replica2Part2Dev []struct {
			ArrayType string // should be "H"
			Data      []uint
		} `pickle:"_replica2part2dev"`
		DispersionGraph map[PickleTuple][]interface{} `pickle:"_dispersion_graph"`
	}
	var dst RingBuilder
	require.Nil(t, Unmarshal(data, &dst))
	data2 := PickleDumps(dst)

	// then load that into an interface{}
	unst2, err := PickleLoads(data2)
	require.Nil(t, err)

	// make sure all the data was preserved in the round-trip.
	require.Equal(t, unst1, unst2)
}

func TestReadAsyncPending(t *testing.T) {
	// real life async pending generated by swift.
	b, err := hex.DecodeString("1f8b080827f53659000365386461653563333033333335636564636" +
		"133343733363835393738333365322d313439363737333334302e39353232380085524d6f9c301" +
		"0bdfb57e4b67b61f107069c5bd2564ad44b94dd957243c61e3624015be06d437f7d67a0cab5b26" +
		"4799edf78fcdeccde47c18ebb57b01ea679c7a264ce85b834135c186e2e8c739aae2e858945c5f" +
		"66efedd77e9e0c23084f1b0a535efb034be77893dacf14f58be53140be69aa6bdf61fa91f9b86a" +
		"d94a8d9dec7126bbe64c7fe0f60c90a0321f050b3f9b83bcf3065771718132206af42fb062e650" +
		"8ff82e9a61005e28213f525fb16c684ccecb4447a4990161be347ef6ceac39807970053d30476a" +
		"07bb9a59dfa01e664874898a2f28529ab4aa9821f8c96b226bcf8c79dec38678f9e208dd4f4598" +
		"39450585e29dd154ed72d072d33ceb5516527bd236649c9cfd0c10413c5a4f1e97cba794d29dee" +
		"6b990d581e312b735af793efb56e44689fcee7c7a6812fe2d376569552705efcaaef0aa35b6e56" +
		"5ed6b6974eb34b87cb385deaeb78fde5bf70ea3cf8ed82b7b81ec29a00d4bf6387af8241a79c97" +
		"71bf747b2176a36274ce8024ca74cad3be5752b4a6f2b0e9e83ebbc90245c0a36a7e72857fbb03" +
		"041eaab3514ad66591c9debda37494e7d8921603504a729d97e5c2d9164c9ff54126f9517a855d" +
		"26c2ee251e1001cfe0211e34ec4bd020000")
	require.Nil(t, err)
	r, err := gzip.NewReader(bytes.NewBuffer(b))
	require.Nil(t, err)
	data, err := ioutil.ReadAll(r)
	require.Nil(t, err)

	type asyncPending struct {
		Headers   map[string]string `pickle:"headers"`
		Object    string            `pickle:"obj"`
		Account   string            `pickle:"account"`
		Container string            `pickle:"container"`
		Op        string            `pickle:"op"`
	}

	var ap asyncPending
	require.Nil(t, Unmarshal(data, &ap))
	require.Equal(t, "object", ap.Object)
	require.Equal(t, "AUTH_test", ap.Account)
	require.Equal(t, "966a3f210f6f4d3b9ab068d8295bc5ec", ap.Container)
	require.Equal(t, "PUT", ap.Op)
	require.Equal(t, "1496773340.95228", ap.Headers["X-Timestamp"])
	require.Equal(t, "tx8e22e4a0735f4c58b0e52-005936f2dc", ap.Headers["X-Trans-Id"])
}
