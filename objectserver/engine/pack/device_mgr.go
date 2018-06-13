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
	"time"

	"github.com/iqiyi/auklet/common/conf"
	"github.com/iqiyi/auklet/common/fs"
	"github.com/iqiyi/auklet/common/ring"

	"go.uber.org/zap"
)

type PackDeviceMgr struct {
	DriveRoot   string
	Policy      int
	Port        int
	hashPrefix  string
	hashSuffix  string
	rwlock      sync.RWMutex
	devices     map[string]*PackDevice
	stopMonitor chan bool
	testMode    bool
}

func NewPackDeviceMgr(port int, driveRoot string, policy int) *PackDeviceMgr {
	prefix, suffix, err := conf.GetHashPrefixAndSuffix()
	if err != nil {
		glogger.Error("unable to find hash prefix/suffix", zap.Error(err))
		return nil
	}

	dm := &PackDeviceMgr{
		DriveRoot:   driveRoot,
		Policy:      policy,
		Port:        port,
		devices:     make(map[string]*PackDevice),
		stopMonitor: make(chan bool),
		hashPrefix:  prefix,
		hashSuffix:  suffix,
	}

	dm.loadPackDevices(policy)

	return dm
}

func (dm *PackDeviceMgr) GetPackDevice(device string) *PackDevice {
	if dm.testMode {
		glogger.Info("get pack device in test mode")
		dm.rwlock.Lock()
		defer dm.rwlock.Unlock()
		d := dm.devices[device]

		if d == nil {
			d = NewPackDevice(device, dm.DriveRoot, dm.Policy)
			dm.devices[device] = d
		}

		return d
	}

	dm.rwlock.RLock()
	defer dm.rwlock.RUnlock()
	return dm.devices[device]
}

func (dm *PackDeviceMgr) Close() {
	dm.rwlock.Lock()
	defer dm.rwlock.Unlock()

	if !dm.testMode {
		dm.stopMonitor <- true
	}

	for name, dev := range dm.devices {
		dev.Close()
		delete(dm.devices, name)
	}
}

func (dm *PackDeviceMgr) loadPackDevices(policy int) {
	devs, err := ring.ListLocalDevices(
		"object", dm.hashPrefix, dm.hashSuffix, dm.Policy, dm.Port)

	if err != nil {
		glogger.Error("unable to get local device list", zap.Int("port", dm.Port))
		panic(err)
	}

	for _, dev := range devs {
		dm.devices[dev.Device] = NewPackDevice(dev.Device, dm.DriveRoot, policy)
	}
}

func (dm *PackDeviceMgr) monitorDisks() {
	mf, err := os.Open("/proc/self/mounts")
	if err != nil {
		glogger.Error("unable to open /proc/self/mounts", zap.Error(err))
		panic(err)
	}
	defer mf.Close()
	mfd := int(mf.Fd())

	epfd, err := syscall.EpollCreate1(0)
	if err != nil {
		glogger.Error("unable to create epoll instance", zap.Error(err))
		panic(err)
	}
	defer syscall.Close(epfd)

	event := &syscall.EpollEvent{
		Events: syscall.EPOLLPRI,
		Fd:     int32(mfd),
	}
	if err = syscall.EpollCtl(
		epfd, syscall.EPOLL_CTL_ADD, mfd, event); err != nil {
		glogger.Error("unable to call EpollCtl", zap.Error(err))
		panic(err)
	}

	events := make([]syscall.EpollEvent, 1)
	for {
		status := make(chan bool)
		go func() {
			if _, err := syscall.EpollWait(epfd, events, -1); err != nil {
				glogger.Error("unable to wait on /proc/self/mounts", zap.Error(err))
				status <- false
				return
			}

			status <- true
		}()

		select {
		case <-dm.stopMonitor:
			glogger.Info("monitor stop signal received")
			return
		case success := <-status:
			if !success {
				time.Sleep(time.Second * 10)
			}
		}

		// Basically, we don't need to worry about that the code executes too
		// frequently here because that means something serious happens in the host
		glogger.Info("disk mount info changed, checking...",
			zap.Int("policy", dm.Policy),
			zap.Int("port", dm.Port))
		dm.checkDisks()
		glogger.Info("disk mount info recheck done",
			zap.Int("policy", dm.Policy),
			zap.Int("port", dm.Port))
	}
}

func (dm *PackDeviceMgr) modifyDevice(device string, d *PackDevice) {
	dm.rwlock.Lock()
	defer dm.rwlock.Unlock()
	if d != nil {
		dm.devices[device] = d
	} else {
		delete(dm.devices, device)
	}
}

func (dm *PackDeviceMgr) checkDisks() {
	devs, err := ring.ListLocalDevices(
		"object", dm.hashPrefix, dm.hashSuffix, dm.Policy, dm.Port)
	if err != nil {
		glogger.Error("unable to get local device list for policy",
			zap.Int("policy", dm.Policy))
		return
	}

	for _, dev := range devs {
		mp := filepath.Join(dm.DriveRoot, dev.Device)
		mounted, err := fs.IsMount(mp)
		if err != nil {
			glogger.Error("unable to check if disk is mounted",
				zap.String("path", mp), zap.Error(err))
			time.Sleep(time.Second * 30)
			continue
		}
		d := dm.GetPackDevice(dev.Device)

		if mounted && d == nil {
			glogger.Debug("pack device of mounted device not found",
				zap.String("device", dev.Device))
			dm.modifyDevice(dev.Device,
				NewPackDevice(dev.Device, dm.DriveRoot, dm.Policy))
			glogger.Info("pack device initialized", zap.String("device", dev.Device))
			continue
		}

		if !mounted && d != nil {
			glogger.Debug("pack device of umounted device found",
				zap.String("device", dev.Device))
			d.Close()
			dm.modifyDevice(dev.Device, nil)
			glogger.Info("pack device of mounted device removed",
				zap.String("device", dev.Device))
		}
	}
}
