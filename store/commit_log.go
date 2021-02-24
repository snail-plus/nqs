package store

import (
	"bytes"
	"github.com/edsrzf/mmap-go"
	log "github.com/sirupsen/logrus"
	"nqs/util"
	"strings"
	"sync"
)

type CommitLog struct {
	putMessageLock        sync.RWMutex
	store                 MessageStore
	mappedFileQueue       *MappedFileQueue
	appendMessageCallback AppendMessageCallback
	flushCommitLogService FlushCommitLogService
}

func NewCommitLog(store MessageStore) CommitLog {
	c := CommitLog{}
	c.store = store
	c.mappedFileQueue = NewMappedFileQueue()
	c.appendMessageCallback = &DefaultAppendMessageCallback{
		msgIdMemory:        bytes.Buffer{},
		msgStoreItemMemory: bytes.Buffer{},
		maxMessageSize:     0,
		keyBuilder:         strings.Builder{},
		msgIdBuilder:       strings.Builder{},
	}

	service := FlushRealTimeService{commitLog: c, stopChan: make(chan struct{})}

	c.flushCommitLogService = service
	return c
}

func (r CommitLog) Start() {
	// TODO 启动定时刷磁盘
	r.flushCommitLogService.start()
}

func (r CommitLog) Shutdown() {
	log.Info("Shutdown commitLog")
	r.flushCommitLogService.shutdown()
	// r.flushCommitLogService.shutdown()
}

func (r CommitLog) PutMessage(inner *MessageExtBrokerInner) *PutMessageResult {
	r.putMessageLock.Lock()
	defer r.putMessageLock.Unlock()

	messageExt := inner.MessageExt
	messageExt.StoreTimestamp = util.GetUnixTime()

	mappedFile := r.mappedFileQueue.GetLastMappedFile()
	if mappedFile == nil || mappedFile.IsFull() {
		mappedFile = r.mappedFileQueue.GetLastMappedFileByOffset(0, true)
	}

	if mappedFile == nil {
		return &PutMessageResult{
			PutMessageStatus: CreateMappedFileFailed,
		}
	}

	appendMessageResult := mappedFile.AppendMessage(inner, r.appendMessageCallback)
	log.Infof("PutMessage ok, topic: %s", inner.Topic)
	return &PutMessageResult{
		PutMessageStatus:    PutOk,
		AppendMessageResult: *appendMessageResult,
	}
}

type DefaultAppendMessageCallback struct {
	msgIdMemory bytes.Buffer

	msgStoreItemMemory bytes.Buffer
	maxMessageSize     int32
	keyBuilder         strings.Builder
	msgIdBuilder       strings.Builder
}

func (r DefaultAppendMessageCallback) DoAppend(fileMap mmap.MMap, currentOffset int32, fileFromOffset int64, maxBlank int32, ext *MessageExtBrokerInner) *AppendMessageResult {
	log.Infof("fileFromOffset %d DoAppend OK", fileFromOffset)
	r.keyBuilder.Reset()
	r.msgIdBuilder.Reset()
	msgStoreItemMemory := r.msgStoreItemMemory
	msgStoreItemMemory.Reset()

	topicData := []byte(ext.Topic)
	topicLength := len(topicData)
	bodyLength := len(ext.Body)

	msgLength := calMsgLength(bodyLength, topicLength)

	// totalSize
	msgStoreItemMemory.Write(util.Int32ToBytes(msgLength))
	msgStoreItemMemory.Write(topicData)
	msgStoreItemMemory.Write(ext.Body)

	copy(fileMap[currentOffset+1:], msgStoreItemMemory.Bytes())

	return &AppendMessageResult{
		Status: AppendOk,
	}
}

func calMsgLength(bodyLength, topicLength int) int {
	return 4 + 4 + 8 + 4 + bodyLength + 1 + topicLength
}
