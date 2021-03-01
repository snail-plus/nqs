package store

import (
	log "github.com/sirupsen/logrus"
	"nqs/common"
	"time"
)

const RetryTimesOver = 3

type FlushConsumeQueue struct {
	common.DaemonTask
	store              *DefaultMessageStore
	lastFlushTimestamp int64
}

func (r *FlushConsumeQueue) start() {
	log.Info("start FlushConsumeQueue service")
	r.Name = "flush service"
	r.DaemonTask.Run = r.run
	r.Start()
}

func (r *FlushConsumeQueue) run() {
	for !r.IsStopped() {
		r.doFlush(RetryTimesOver)
		time.Sleep(1 * time.Second)
	}
}

func (r *FlushConsumeQueue) doFlush(retryTimes int) {
	table := r.store.consumeQueueTable
	for _, v := range table {
		for _, cq := range v {
			result := false
			for i := 0; i < retryTimes && !result; i++ {
				result = cq.Flush()
			}
		}
	}
}

func (r FlushConsumeQueue) shutdown() {
	log.Info("shutdown FlushConsumeQueue service")
	r.Stop()
}
