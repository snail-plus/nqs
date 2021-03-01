package store

import (
	"bytes"
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"math"
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
	result := r.putMessagePositionInfo(request.commitLogOffset, request.msgSize, tagsCode, request.consumeQueueOffset)
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

	// 索引文件第一次创建的时候校正 最小LogicOffset(后边offset从此处开始往后推进) cqOffset 为0 时无需校对
	if mappedFile.firstCreateInQueue && cqOffset != 0 && mappedFile.GetWrotePosition() == 0 {
		r.minLogicOffset = expectLogicOffset
		r.mappedFileQueue.flushedWhere = expectLogicOffset
		// 从 expectLogicOffset 开始建索引 前边内容不需要所以填充空白 并且设置 flush索引 这里空白数据丢失无所谓
		r.fillPreBlank(mappedFile, expectLogicOffset)
		log.Infof("fill blank")
	}

	if cqOffset != 0 {
		currentLogicOffset := int64(mappedFile.GetWrotePosition()) + mappedFile.fileFromOffset
		if expectLogicOffset < currentLogicOffset {
			log.Info("Build  consume queue repeatedly")
			return true
		}

		if expectLogicOffset != currentLogicOffset {
			log.Infof("[bug] logic queue order maybe wrong, expectLogicOffset: %d currentLogicOffset:  %d, cqOffset: %d", expectLogicOffset, currentLogicOffset, cqOffset)
		}
	}

	r.maxPhysicOffset = offset + int64(size)
	return mappedFile.AppendMessageBytes(r.byteBufferIndex.Bytes())
}

func (r *ConsumeQueue) fillPreBlank(mappedFile *MappedFile, untilWhere int64) {
	blankByte := make([]byte, CqStoreUnitSize)
	byteBuffer := bytes.NewBuffer(blankByte[:0])
	binary.Write(byteBuffer, binary.BigEndian, int64(0))
	binary.Write(byteBuffer, binary.BigEndian, int32(math.MaxInt32))
	binary.Write(byteBuffer, binary.BigEndian, int64(0))

	until := untilWhere % int64(r.mappedFileQueue.mappedFileSize)
	for i := 0; i < int(until); i += CqStoreUnitSize {
		mappedFile.AppendMessageBytes(byteBuffer.Bytes())
	}
}

func (r *ConsumeQueue) Flush() bool {
	return r.mappedFileQueue.Flush()
}

func (r ConsumeQueue) GetMinOffsetInQueue() int64 {
	return r.minLogicOffset / CqStoreUnitSize
}

func (r ConsumeQueue) GetMaxOffsetInQueue() int64 {
	return r.mappedFileQueue.GetMaxOffset() / CqStoreUnitSize
}

func (r ConsumeQueue) GetIndexBuffer(startIndex int64) *SelectMappedBufferResult {
	mappedFileSize := mappedFileSizeConsumeQueue
	offset := startIndex * CqStoreUnitSize
	if offset >= r.GetMinOffsetInQueue() {
		mappedFile := r.mappedFileQueue.findMappedFileByOffset(offset, false)
		if mappedFile != nil {
			return mappedFile.selectMappedBuffer(int32(offset % int64(mappedFileSize)))
		}
	}

	return nil
}
