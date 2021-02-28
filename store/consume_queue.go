package store

import (
	"bytes"
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"strconv"
)

const (
	CqStoreUnitSize = 20
)

type ConsumeQueue struct {
	defaultMessageStore *DefaultMessageStore
	mappedFileQueue     *MappedFileQueue
	topic               string
	queueId             int32
	storePath           string
	maxPhysicOffset     int64
	minLogicOffset      int64

	byteBufferIndex *bytes.Buffer
}

func NewConsumeQueue(defaultMessageStore *DefaultMessageStore,
	topic string,
	queueId int32,
	storePath string,
) *ConsumeQueue {
	queueDir := storePath + "/" + topic + "/" + strconv.Itoa(int(queueId))
	mappedFileQueue := NewMappedFileQueue(queueDir, mappedFileSizeConsumeQueue)

	return &ConsumeQueue{
		defaultMessageStore: defaultMessageStore,
		mappedFileQueue:     mappedFileQueue,
		topic:               topic,
		queueId:             queueId,
		storePath:           storePath,
		maxPhysicOffset:     -1,
		minLogicOffset:      0,
		byteBufferIndex:     bytes.NewBuffer(make([]byte, CqStoreUnitSize)),
	}

}

func (r *ConsumeQueue) putMessagePositionInfoWrapper(request *DispatchRequest) {
	tagsCode := request.tagsCode
	result := r.putMessagePositionInfo(request.commitLogOffset, request.msgSize, tagsCode, request.commitLogOffset)
	if !result {
		log.Errorf("putMessagePositionInfo error")
		return
	}
}

func (r *ConsumeQueue) putMessagePositionInfo(offset int64, size int32, tagsCode, cqOffset int64) bool {
	r.byteBufferIndex.Reset()
	binary.Write(r.byteBufferIndex, binary.BigEndian, offset)
	binary.Write(r.byteBufferIndex, binary.BigEndian, size)
	binary.Write(r.byteBufferIndex, binary.BigEndian, tagsCode)

	expectLogicOffset := cqOffset * CqStoreUnitSize
	mappedFile := r.mappedFileQueue.GetLastMappedFileByOffset(expectLogicOffset, true)
	if mappedFile == nil {
		return false
	}

	return false
}
