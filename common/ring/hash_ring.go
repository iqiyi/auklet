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

package ring

import (
	"compress/gzip"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type hashRing struct {
	data   atomic.Value
	path   string
	prefix string
	suffix string
	mtime  time.Time
}

func (r *hashRing) getData() *ringData {
	return r.data.Load().(*ringData)
}

func (r *hashRing) GetNodes(partition uint64) []*Device {
	d := r.getData()
	if partition >= uint64(len(d.replica2part2devId[0])) {
		return nil
	}
	var devs []*Device
	for i := 0; i < d.ReplicaCount; i++ {
		devs = append(devs, d.Devs[d.replica2part2devId[i][partition]])
	}
	return devs
}

func (r *hashRing) GetJobNodes(
	partition uint64, localDevice int) ([]*Device, bool) {

	d := r.getData()
	if partition >= uint64(len(d.replica2part2devId[0])) {
		return nil, false
	}

	var devs []*Device
	handoff := true
	for i := 0; i < d.ReplicaCount; i++ {
		dev := d.Devs[d.replica2part2devId[i][partition]]
		if dev.Id == localDevice {
			handoff = false
		} else {
			devs = append(devs, dev)
		}
	}

	return devs, handoff
}

func (r *hashRing) GetPartition(
	account string, container string, object string) uint64 {

	d := r.getData()
	hash := md5.New()

	hash.Write([]byte(r.prefix + "/" + account))

	if container != "" {
		hash.Write([]byte("/" + container))
	}

	if object != "" {
		hash.Write([]byte("/" + object))
	}

	hash.Write([]byte(r.suffix))

	digest := hash.Sum(nil)
	// treat as big endian unsigned int
	val := uint64(digest[0])<<24 |
		uint64(digest[1])<<16 |
		uint64(digest[2])<<8 |
		uint64(digest[3])

	return val >> d.PartShift
}

func (r *hashRing) LocalDevices(localPort int) ([]*Device, error) {
	localAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	localIPs := make(map[string]bool)
	for _, addr := range localAddrs {
		localIPs[strings.Split(addr.String(), "/")[0]] = true
	}

	d := r.getData()
	var devs []*Device
	for _, dev := range d.Devs {
		if dev != nil && localIPs[dev.ReplicationIp] && dev.ReplicationPort == localPort {
			devs = append(devs, dev)
		}
	}

	return devs, nil
}

func (r *hashRing) GetMoreNodes(partition uint64) MoreNodes {
	return &hashMoreNodes{r: r, partition: partition, used: nil}
}

func (r *hashRing) ReplicaCount() (cnt uint64) {
	d := r.getData()
	return uint64(len(d.replica2part2devId))
}

func (r *hashRing) PartitionCount() (cnt uint64) {
	d := r.getData()
	return uint64(len(d.replica2part2devId[0]))
}

func (r *hashRing) reloader() error {
	for {
		time.Sleep(reloadTime)
		r.reload()
	}
}
func (r *hashRing) reload() error {
	fi, err := os.Stat(r.path)
	if err != nil {
		return err
	}

	if fi.ModTime() == r.mtime {
		return nil
	}

	fp, err := os.Open(r.path)
	if err != nil {
		return err
	}

	gz, err := gzip.NewReader(fp)
	if err != nil {
		return err
	}

	magicBuf := make([]byte, 4)
	io.ReadFull(gz, magicBuf)
	if string(magicBuf) != "R1NG" {
		return ErrBadMagicNumber
	}

	var ringVersion uint16
	binary.Read(gz, binary.BigEndian, &ringVersion)
	if ringVersion != 1 {
		return ErrUnknownRingVersion
	}

	var jsonLen uint32
	binary.Read(gz, binary.BigEndian, &jsonLen)
	jsonBuf := make([]byte, jsonLen)
	if _, err := io.ReadFull(gz, jsonBuf); err != nil {
		return err
	}

	data := &ringData{}
	if err := json.Unmarshal(jsonBuf, data); err != nil {
		return err
	}

	partitionCount := 1 << (32 - data.PartShift)
	for i := 0; i < data.ReplicaCount; i++ {
		part2dev := make([]uint16, partitionCount)
		binary.Read(gz, binary.LittleEndian, &part2dev)
		data.replica2part2devId = append(data.replica2part2devId, part2dev)
	}
	regionCount := make(map[int]bool)
	zoneCount := make(map[regionZone]bool)
	ipPortCount := make(map[ipPort]bool)
	for _, d := range data.Devs {
		if d != nil {
			regionCount[d.Region] = true
			zoneCount[regionZone{d.Region, d.Zone}] = true
			ipPortCount[ipPort{d.Region, d.Zone, d.Port, d.Ip}] = true
		}
	}
	data.regionCount = len(regionCount)
	data.zoneCount = len(zoneCount)
	data.ipPortCount = len(ipPortCount)
	r.mtime = fi.ModTime()
	r.data.Store(data)
	return nil
}
