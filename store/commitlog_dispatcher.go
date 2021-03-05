package store

type CommitLogDispatcher interface {
	Dispatch(request *DispatchRequest)
}

type CommitLogDispatcherBuildConsumeQueue struct {
	store *DefaultMessageStore
	// TODO golang 不能循环引用这里采用反射调用msgStore 对应方法
}

func (r CommitLogDispatcherBuildConsumeQueue) Dispatch(request *DispatchRequest) {
	r.store.putMessagePositionInfo(request)
}
