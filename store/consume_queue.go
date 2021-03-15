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
			log.Warnf("Build  consume queue repeatedly, expectLogicOffset: %d currentLogicOffset:  %d, cqOffset: %d", expectLogicOffset, currentLogicOffset, cqOffset)
			return true
		}

		if expectLogicOffset != currentLogicOffset {
			log.Infof("[bug] logic queue order maybe wrong, expectLogicOffset: %d currentLogicOffset:  %d, cqOffset: %d", expectLogicOffset, currentLogicOffset, cqOffset)
		}
	}

	r.maxPhysicOffset = offset + int64(size)
	msgBytes := r.byteBufferIndex.Bytes()
	appendResult := mappedFile.AppendMessageBytes(msgBytes)
	log.Infof("ConsumeQueue maxOffset: %d", r.GetMaxOffsetInQueue())
	return appendResult
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

func (r *ConsumeQueue) load() bool {
	result := r.mappedFileQueue.Load()
	return result
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

func (r *ConsumeQueue) recover() {
	mappedFiles := r.mappedFileQueue.mappedFiles
	if mappedFiles == nil || mappedFiles.Len() == 0 {
		log.Info("ConsumeQueue file is empty")
		return
	}

	// 从倒数第二个文件开始查看 这里从倒数第三个是防止文件破损
	index := mappedFiles.Len() - 3
	if index < 0 {
		index = 0
	}

	mappedFile := r.mappedFileQueue.getMappedFileByIndex(index)
	buffer := mappedFile.GetFileBuffer()

	processOffset := mappedFile.fileFromOffset
	var mappedFileOffset int64 = 0

	for {
		for i := 0; i < mappedFileSizeConsumeQueue; i += CqStoreUnitSize {
			var offset int64
			binary.Read(buffer, binary.BigEndian, &offset)
			var size int32
			binary.Read(buffer, binary.BigEndian, &size)

			if i == 0 && size == 0 {
				log.Infof("当前文件: %s 内容有错误, 起始文件头部为o", mappedFile.fileName)
				break
			}

			var tagsCode int64
			binary.Read(buffer, binary.BigEndian, &tagsCode)

			if size <= 0 {
				mappedFile.wrotePosition = int32(i)
				log.Infof("recover current consume queue file over1,file: %s", mappedFile.fileName)
				break
			}

			mappedFileOffset = int64(i) + int64(CqStoreUnitSize)
			r.maxPhysicOffset = offset + int64(size)
		}

		// 不等 说明该队列索引文件只是写了部分 是最后一次写的文件 终止恢复
		if mappedFileOffset != mappedFileSizeConsumeQueue {
			log.Infof("recover current consume queue file over2,file: %s", mappedFile.fileName)
			break
		}

		index++
		if index >= mappedFiles.Len() {
			log.Infof("recover last consume queue file over, file: %s", mappedFile.fileName)
			break
		}

		mappedFile = r.mappedFileQueue.getMappedFileByIndex(index)
		buffer = mappedFile.GetFileBuffer()

		processOffset = mappedFile.fileFromOffset
		mappedFileOffset = 0
		log.Infof("recover next consume queue file over, file: %s", mappedFile.fileName)
	}

	processOffset += mappedFileOffset
	r.mappedFileQueue.flushedWhere = processOffset
	mappedFile.wrotePosition = int32(mappedFileOffset)
	log.Infof("ConsumeQueue offset: %d, flushedWhere: %d,wrotePosition: %d", processOffset/CqStoreUnitSize, processOffset, mappedFile.wrotePosition)

}

func (r *ConsumeQueue) truncateDirtyLogicFiles(phyOffset int64) {
	logicFileSize := mappedFileSizeConsumeQueue
	for {
		mappedFile := r.mappedFileQueue.GetLastMappedFile()
		if mappedFile == nil {
			return
		}

		mappedFile.wrotePosition = 0
		mappedFile.flushedPosition = 0
		byteBuffer := mappedFile.GetFileBuffer()

		for i := 0; i < logicFileSize; i += CqStoreUnitSize {
			var offset int64
			binary.Read(byteBuffer, binary.BigEndian, &offset)
			var size int32
			binary.Read(byteBuffer, binary.BigEndian, &size)
			var tagsCode int64
			binary.Read(byteBuffer, binary.BigEndian, &tagsCode)

			if i == 0 {
				// 第一条索引的offset 大于commitLog 最大offset 说明该文件多余删除
				if offset >= phyOffset {
					r.mappedFileQueue.DeleteLastMappedFile()
					break
				} else {
					pos := i + CqStoreUnitSize
					mappedFile.wrotePosition = int32(pos)
					mappedFile.flushedPosition = int32(pos)
					r.maxPhysicOffset = offset + int64(size)
				}
			} else {
				// 消息大小为0 说明无效直接返回
				if offset < 0 || size <= 0 {
					return
				}

				// offset >= phyOffset 说明索引文件已经恢复到指定pos
				if offset >= phyOffset {
					log.Infof("最终索引文件: %s, offset: %d", mappedFile.fileName, offset)
					return
				}

				pos := i + CqStoreUnitSize
				mappedFile.wrotePosition = int32(pos)
				mappedFile.flushedPosition = int32(pos)
				r.maxPhysicOffset = offset + int64(size)
				// 说明到了改文件最后位置 这里返回说明 索引文件offset 真实索引位置比 commitLog 要小
				if pos == logicFileSize {
					return
				}

			}
		}

	}
}

func (r *ConsumeQueue) Shutdown() {
	r.mappedFileQueue.Shutdown()
}
