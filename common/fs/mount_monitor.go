package fs

import (
	"os"
	"sync"
	"syscall"
	"time"
)

type MountMonitor struct {
	stop      chan bool
	callbacks map[string]func()
	lock      sync.RWMutex
}

func (m *MountMonitor) monitor() {
	mf, err := os.Open("/proc/self/mounts")
	if err != nil {
		panic(err)
	}
	defer mf.Close()
	mfd := int(mf.Fd())

	epfd, err := syscall.EpollCreate1(0)
	if err != nil {
		panic(err)
	}
	defer syscall.Close(epfd)

	event := &syscall.EpollEvent{
		Events: syscall.EPOLLPRI,
		Fd:     int32(mfd),
	}
	if err = syscall.EpollCtl(
		epfd, syscall.EPOLL_CTL_ADD, mfd, event); err != nil {
		panic(err)
	}

	events := make([]syscall.EpollEvent, 1)
	for {
		status := make(chan bool)
		go func() {
			if _, err := syscall.EpollWait(epfd, events, -1); err != nil {
				status <- false
				return
			}
			status <- true
		}()

		select {
		case <-m.stop:
			return
		case success := <-status:
			if !success {
				time.Sleep(time.Second * 10)
			}
		}

		func() {
			m.lock.RLock()
			defer m.lock.RUnlock()
			for _, c := range m.callbacks {
				c()
			}
		}()
	}
}

func (m *MountMonitor) Start() {
	go m.monitor()
}

func (m *MountMonitor) Stop() {
	m.stop <- true
}

func (m *MountMonitor) RegisterCallback(name string, callback func()) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.callbacks[name] = callback
}

func (m *MountMonitor) UnregisterCallback(name string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.callbacks, name)
}

func NewMountMonitor() *MountMonitor {
	return &MountMonitor{
		stop:      make(chan bool),
		callbacks: map[string]func(){},
		lock:      sync.RWMutex{},
	}
}
