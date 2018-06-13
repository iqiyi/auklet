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

package ring

import (
	"crypto/md5"
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"
)

const reloadTime = 15 * time.Second

type Ring interface {
	GetNodes(partition uint64) []*Device

	GetJobNodes(partition uint64, localDevice int) ([]*Device, bool)

	GetPartition(account string, container string, object string) uint64

	LocalDevices(localPort int) ([]*Device, error)

	GetMoreNodes(partition uint64) MoreNodes

	ReplicaCount() (cnt uint64)

	PartitionCount() (cnt uint64)
}

type MoreNodes interface {
	Next() *Device
}

type ringData struct {
	Devs               []*Device `json:"devs"`
	ReplicaCount       int       `json:"replica_count"`
	PartShift          uint64    `json:"part_shift"`
	replica2part2devId [][]uint16
	regionCount        int
	zoneCount          int
	ipPortCount        int
}

type regionZone struct {
	region int
	zone   int
}

type ipPort struct {
	region int
	zone   int
	port   int
	ip     string
}

type hashMoreNodes struct {
	r           *hashRing
	used        map[int]bool
	sameRegions map[int]bool
	sameZones   map[regionZone]bool
	sameIpPorts map[ipPort]bool
	parts       int
	start       int
	inc         int
	partition   uint64
}

func (m *hashMoreNodes) addDevice(d *Device) {
	m.used[d.Id] = true
	m.sameRegions[d.Region] = true
	m.sameZones[regionZone{d.Region, d.Zone}] = true
	m.sameIpPorts[ipPort{d.Region, d.Zone, d.Port, d.Ip}] = true
}

func (m *hashMoreNodes) initialize() {
	d := m.r.getData()
	m.parts = len(d.replica2part2devId[0])
	m.used = make(map[int]bool)
	m.sameRegions = make(map[int]bool)
	m.sameZones = make(map[regionZone]bool)
	m.sameIpPorts = make(map[ipPort]bool)
	for _, mp := range d.replica2part2devId {
		m.addDevice(d.Devs[mp[m.partition]])
	}
	hash := md5.New()
	hash.Write([]byte(strconv.FormatUint(m.partition, 10)))
	digest := hash.Sum(nil)
	m.start = int((uint64(digest[0])<<24 | uint64(digest[1])<<16 | uint64(digest[2])<<8 | uint64(digest[3])) >> d.PartShift)
	m.inc = m.parts / 65536
	if m.inc == 0 {
		m.inc = 1
	}
}

func (m *hashMoreNodes) Next() *Device {
	d := m.r.getData()
	if m.used == nil {
		m.initialize()
	}
	var check func(d *Device) bool
	if len(m.sameRegions) < d.regionCount {
		check = func(d *Device) bool { return !m.sameRegions[d.Region] }
	} else if len(m.sameZones) < d.zoneCount {
		check = func(d *Device) bool { return !m.sameZones[regionZone{d.Region, d.Zone}] }
	} else if len(m.sameIpPorts) < d.ipPortCount {
		check = func(d *Device) bool { return !m.sameIpPorts[ipPort{d.Region, d.Zone, d.Port, d.Ip}] }
	} else {
		check = func(d *Device) bool { return !m.used[d.Id] }
	}
	for i := 0; i < m.parts; i += m.inc {
		handoffPart := (i + m.start) % m.parts
		for _, part2devId := range d.replica2part2devId {
			if handoffPart < len(part2devId) {
				if check(d.Devs[part2devId[handoffPart]]) {
					m.addDevice(d.Devs[part2devId[handoffPart]])
					return d.Devs[part2devId[handoffPart]]
				}
			}
		}
	}
	return nil
}

var loadedRingsLock sync.Mutex
var loadedRings map[string]*hashRing = make(map[string]*hashRing)

func LoadRing(path string, prefix string, suffix string) (Ring, error) {
	loadedRingsLock.Lock()
	defer loadedRingsLock.Unlock()
	ring := loadedRings[path]
	if ring == nil {
		ring = &hashRing{prefix: prefix, suffix: suffix, path: path, mtime: time.Unix(0, 0)}
		if err := ring.reload(); err != nil {
			return nil, err
		}
		go ring.reloader()
		loadedRings[path] = ring
	}
	return ring, nil
}

// GetRing returns the current ring given the ring_type ("account", "container", "object"),
// hash path prefix, and hash path suffix. An error is raised if the requested ring does
// not exist.
func GetRing(ringType, prefix, suffix string, policy int) (Ring, error) {
	var ring Ring
	var err error
	ringFile := fmt.Sprintf("%s.ring.gz", ringType)
	if policy != 0 {
		ringFile = fmt.Sprintf("%s-%d.ring.gz", ringType, policy)
	}
	if ring, err = LoadRing(fmt.Sprintf("/etc/auklet/%s", ringFile), prefix, suffix); err != nil {
		if ring, err = LoadRing(fmt.Sprintf("/etc/swift/%s", ringFile), prefix, suffix); err != nil {
			return nil, fmt.Errorf("Error loading %s:%d ring", ringType, policy)
		}
	}
	return ring, nil
}

func ListLocalDevices(ringType, prefix, suffix string, policy int,
	port int) ([]*Device, error) {
	name := fmt.Sprintf("%s.ring.gz", ringType)
	if policy != 0 {
		name = fmt.Sprintf("%s-%d.ring.gz", ringType, policy)
	}
	ring := &hashRing{
		prefix: prefix,
		suffix: suffix,
		mtime:  time.Unix(0, 0),
	}

	var err error
	for _, dir := range configDirs {
		ring.path = path.Join(dir, name)
		if err = ring.reload(); err == nil {
			break
		}
	}

	if err != nil {
		return nil, err
	}

	return ring.LocalDevices(port)
}
