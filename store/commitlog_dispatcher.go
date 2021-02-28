package store

import log "github.com/sirupsen/logrus"

type CommitLogDispatcher interface {
	Dispatch(request *DispatchRequest)
}

type CommitLogDispatcherBuildConsumeQueue struct {
}

func (r CommitLogDispatcherBuildConsumeQueue) Dispatch(request *DispatchRequest) {
	log.Infof("build ConsumeQueue request")
}
