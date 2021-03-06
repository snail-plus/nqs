package store

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
)

type PutMessageLock interface {
	Lock()
	UnLock()
}

type PutMessageReentrantLock struct {
	lock sync.Mutex
}

func (r *PutMessageReentrantLock) Lock() {
	r.lock.Lock()
}

func (r *PutMessageReentrantLock) UnLock() {
	r.lock.Unlock()
}

type PutMessageSpinLock struct {
	value int32
}

func (r *PutMessageSpinLock) Lock() {
	for {
		if atomic.CompareAndSwapInt32(&r.value, 0, 1) {
			break
		}
	}
}

func (r *PutMessageSpinLock) UnLock() {
	swapResult := atomic.CompareAndSwapInt32(&r.value, 1, 0)
	if !swapResult {
		log.Warn("[BUG] 解锁失败")
	}
}
