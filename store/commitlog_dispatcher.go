package store

import log "github.com/sirupsen/logrus"

type CommitLogDispatcher interface {
	Dispatch(request *DispatchRequest)
}

type CommitLogDispatcherBuildConsumeQueue struct {
	store *DefaultMessageStore
	// TODO golang 不能循环引用这里采用反射调用msgStore 对应方法
}

func (r CommitLogDispatcherBuildConsumeQueue) Dispatch(request *DispatchRequest) {
	log.Infof("build ConsumeQueue request")
	r.store.putMessagePositionInfo(request)
}
