package store

type FlushRealTimeService struct {
	defaultMessageStore DefaultMessageStore
}

func (r FlushRealTimeService) Start() {
	go func() {
		r.defaultMessageStore.commitLog.mappedFileQueue.Flush()
	}()
}
