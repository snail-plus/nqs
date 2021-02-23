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
	stopChan   chan struct{}
	c          CommitLog
	printTimes int64
}

func (r FlushRealTimeService) start() {
	log.Info("start flush service")

	go func(flush FlushRealTimeService) {
		for {
			printFlushProgress := false
			select {
			case <-flush.stopChan:
				log.Infof("收到退出信号")
				break
			default:
				commitLog := flush.c
				flushResult := commitLog.mappedFileQueue.Flush()

				flush.printTimes = flush.printTimes + 1
				printFlushProgress = (flush.printTimes % int64(100)) == 0
				if printFlushProgress {
					log.Infof("flushResult : %v", flushResult)
				}

				time.Sleep(1 * time.Second)
			}
		}
	}(r)
}

func (r FlushRealTimeService) shutdown() {
	log.Info("shutdown flush service")
	r.stopChan <- struct{}{}
}
