package store

import (
	"bytes"
	"github.com/edsrzf/mmap-go"
	log "github.com/sirupsen/logrus"
	"nqs/util"
	"strings"
	"sync"
)

const (
	MessageMagicCode = -626843481
	BlankMagicCode   = -875286124
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
	c.mappedFileQueue = NewMappedFileQueue("commitlog")
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

func (r CommitLog) Load() bool {
	return r.mappedFileQueue.Load()
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

func (r CommitLog) GetMinOffset() int64 {
	return r.mappedFileQueue.GetMinOffset()
}

func (r CommitLog) GetMaxOffset() int64 {
	return r.mappedFileQueue.GetMaxOffset()
}

func (r CommitLog) GetData(offset int64, returnFirstOnNotFound bool) {

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
	propertiesData := ext.propertiesString
	propertiesLength := len(propertiesData)

	var bodyLength int
	if ext.Body == nil {
		bodyLength = 0
	} else {
		bodyLength = len(ext.Body)
	}

	msgLength := calMsgLength(bodyLength, topicLength, propertiesLength)

	// 1 totalSize 4
	msgStoreItemMemory.Write(util.Int32ToBytes(msgLength))

	// 2 magicCode 4
	msgStoreItemMemory.Write(util.Int32ToBytes(MessageMagicCode))
	// 3 bodyCrc 4
	msgStoreItemMemory.Write(ext.GetBody())
	// 4 queueId 4
	msgStoreItemMemory.Write(util.Int32ToBytes(int(ext.QueueId)))
	// 5 flag 4
	msgStoreItemMemory.Write(util.Int32ToBytes(int(ext.Flag)))
	// 6 queueOffset 8
	msgStoreItemMemory.Write(util.Int64ToBytes(ext.QueueOffset))
	// 7 physicalOffset 8
	msgStoreItemMemory.Write(util.Int64ToBytes(fileFromOffset + int64(currentOffset)))
	// 8 sysFlag 4

	msgStoreItemMemory.Write(util.Int32ToBytes(int(ext.SysFlag)))
	// 9 bornTimestamp 8
	msgStoreItemMemory.Write(util.Int64ToBytes(ext.BornTimestamp))
	// 10 bornHost 8
	msgStoreItemMemory.Write(util.AddressToByte(ext.BornHost))
	// 11 storeTimestamp 8
	msgStoreItemMemory.Write(util.Int64ToBytes(ext.StoreTimestamp))
	// 12 storeHostAddress 8
	msgStoreItemMemory.Write(util.AddressToByte(ext.StoreHost))
	// 13 reconsumeTimes

	msgStoreItemMemory.Write(util.Int32ToBytes(int(ext.ReconsumeTimes)))
	// 14 Prepared transaction offset
	msgStoreItemMemory.Write(util.Int64ToBytes(ext.PreparedTransactionOffset))
	// 15 body length 4
	msgStoreItemMemory.Write(util.Int32ToBytes(bodyLength))
	if bodyLength > 0 {
		msgStoreItemMemory.Write(ext.Body)
	}
	// 16 topic
	msgStoreItemMemory.Write(util.Int8ToBytes(topicLength))
	msgStoreItemMemory.Write(topicData)
	// 17 properties
	msgStoreItemMemory.Write(util.Int16ToBytes(propertiesLength))
	if propertiesLength > 0 {
		msgStoreItemMemory.Write([]byte(propertiesData))
	}

	copyLength := copy(fileMap, msgStoreItemMemory.Bytes())
	log.Infof("copyLength: %d", copyLength)

	return &AppendMessageResult{
		Status: AppendOk,
	}
}

func calMsgLength(bodyLength, topicLength, propertiesLength int) int {
	bornHostLength := 8
	storeHostAddressLength := 8

	return 4 /*totalSize*/ +
		4 /*magicCode*/ +
		4 /**bodyCrc */ +
		4 /*queueId*/ +
		4 /* flag*/ +
		8 /* QUEUEOFFSET*/ +
		8 /*PHYSICALOFFSET*/ +
		4 /*SYSFLAG*/ +
		8 /*BORNTIMESTAMP*/ +
		bornHostLength /*BORNHOST*/ +
		8 /*STORETIMESTAMP*/ +
		storeHostAddressLength /*STOREHOSTADDRESS*/ +
		4 /*RECONSUMETIMES*/ +
		8 /*Prepared Transaction Offset*/ +
		4 + bodyLength /*BODY*/ +
		1 + topicLength /*TOPIC*/ +
		2 + propertiesLength //propertiesLength
}
