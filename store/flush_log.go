package store

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type FlushCommitLogService interface {
	shutdown()
	start()
}

type FlushRealTimeService struct {
	stopChan chan struct{}
	c        CommitLog
}

func (r FlushRealTimeService) start() {
	log.Info("start flush service")

	go func(flush FlushRealTimeService) {
		for {
			select {
			case <-flush.stopChan:
				log.Infof("收到退出信号")
				break
			default:
				time.Sleep(1 * time.Second)
			}
		}
	}(r)
}

func (r FlushRealTimeService) shutdown() {
	log.Info("shutdown flush service")
	r.stopChan <- struct{}{}
}
