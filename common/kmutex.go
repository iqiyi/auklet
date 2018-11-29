package common

import (
	"sync"
)

type Kmutex struct {
	m *sync.Map
}

func NewKmutex() *Kmutex {
	m := sync.Map{}
	return &Kmutex{&m}
}

func (k *Kmutex) Lock(key interface{}) {
	m := sync.Mutex{}
	m_, _ := k.m.LoadOrStore(key, &m)
	mm := m_.(*sync.Mutex)
	mm.Lock()
}

func (k *Kmutex) Unlock(key interface{}) {
	l, exist := k.m.Load(key)
	if !exist {
		return
	}
	l_ := l.(*sync.Mutex)
	k.m.Delete(key)
	l_.Unlock()
}

