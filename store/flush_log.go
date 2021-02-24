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
	commitLog  CommitLog
	printTimes int64
}

func (r FlushRealTimeService) start() {
	log.Info("start flush service")

	go func(flushService FlushRealTimeService) {
		for {
			printFlushProgress := false
			select {
			case <-flushService.stopChan:
				log.Infof("收到退出信号")
				break
			default:
				commitLog := flushService.commitLog
				flushResult := commitLog.mappedFileQueue.Flush()

				flushService.printTimes = flushService.printTimes + 1
				printFlushProgress = (flushService.printTimes % int64(100)) == 0
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
