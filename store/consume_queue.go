package store

import "strconv"

type ConsumeQueue struct {
	defaultMessageStore *DefaultMessageStore
	mappedFileQueue     *MappedFileQueue
	topic               string
	queueId             int32
	storePath           string
	maxPhysicOffset     int64
	minLogicOffset      int64
}

func NewConsumeQueue(defaultMessageStore *DefaultMessageStore,
	topic string,
	queueId int32,
	storePath string,
) *ConsumeQueue {
	queueDir := storePath + "/" + topic + "/" + strconv.Itoa(int(queueId))
	mappedFileQueue := NewMappedFileQueue(queueDir)

	return &ConsumeQueue{
		defaultMessageStore: defaultMessageStore,
		mappedFileQueue:     mappedFileQueue,
		topic:               topic,
		queueId:             queueId,
		storePath:           storePath,
		maxPhysicOffset:     0,
		minLogicOffset:      0,
	}

}

func (r *ConsumeQueue) putMessagePositionInfoWrapper(request *DispatchRequest) {

}
