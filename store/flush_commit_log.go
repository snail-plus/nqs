package store

import (
	log "github.com/sirupsen/logrus"
	"nqs/common"
	"time"
)

type FlushCommitLogService interface {
	shutdown()
	start()
}

type FlushRealTimeService struct {
	common.DaemonTask
	stopChan   chan struct{}
	commitLog  CommitLog
	printTimes int64
}

func (r *FlushRealTimeService) start() {
	log.Info("start commitLog flush service")
	r.Name = "flush service"
	r.DaemonTask.Run = r.run
	r.Start()
}

func (r *FlushRealTimeService) run() {
	flushService := r
	printFlushProgress := false
	for !r.IsStopped() {
		commitLog := flushService.commitLog
		flushResult := commitLog.mappedFileQueue.Flush()

		flushService.printTimes = flushService.printTimes + 1
		printFlushProgress = (flushService.printTimes % int64(100)) == 0
		if printFlushProgress {
			log.Infof("flushResult : %v", flushResult)
		}
		time.Sleep(1 * time.Second)
	}

	//shutdown 继续尝试刷新
	var result = false
	for i := 0; i < 10 && !result; i++ {
		result = r.commitLog.mappedFileQueue.Flush()
	}
	log.Infof("FlushRealTimeService end, flush result: %t", result)
}

func (r FlushRealTimeService) shutdown() {
	log.Info("shutdown flush service")
	r.Stop()
}
