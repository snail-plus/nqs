package store

import (
	"bytes"
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	log "github.com/sirupsen/logrus"
	"nqs/util"
	"strconv"
	"strings"
	"sync"
)

const (
	MessageMagicCode = -626843481
	BlankMagicCode   = -875286124
)

var topicQueueTable = map[string]int64{}

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
	c.mappedFileQueue = NewMappedFileQueue(BasePath+"/commitlog", commitLogFileSize)
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

func (r CommitLog) GetData(offset int64, returnFirstOnNotFound bool) *SelectMappedBufferResult {
	mappedFile := r.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound)
	if mappedFile == nil {
		return nil
	}
	pos := offset % commitLogFileSize
	return mappedFile.selectMappedBuffer(int32(pos))
}

func (r CommitLog) CheckMessage(byteBuff *bytes.Buffer, checkCrc, readBody bool) *DispatchRequest {

	var totalSize int32
	binary.Read(byteBuff, binary.BigEndian, &totalSize)

	var magicCode int32
	binary.Read(byteBuff, binary.BigEndian, &magicCode)
	switch magicCode {
	case MessageMagicCode:
		break
	case BlankMagicCode:
		return &DispatchRequest{
			msgSize: 0,
			success: true,
		}
	default:
		log.Warnf("illegal magic code: %d", magicCode)
		return &DispatchRequest{
			msgSize: 0,
			success: false,
		}
	}

	var bodyCRC int32
	binary.Read(byteBuff, binary.BigEndian, &bodyCRC)

	var queueId int32
	binary.Read(byteBuff, binary.BigEndian, &queueId)

	var flag int32
	binary.Read(byteBuff, binary.BigEndian, &flag)

	var queueOffset int64
	binary.Read(byteBuff, binary.BigEndian, &queueOffset)

	var physicOffset int64
	binary.Read(byteBuff, binary.BigEndian, &physicOffset)

	var sysFlag int32
	binary.Read(byteBuff, binary.BigEndian, &sysFlag)

	var bornTimeStamp int64
	binary.Read(byteBuff, binary.BigEndian, &bornTimeStamp)

	bornHostAddress := make([]byte, 8)
	byteBuff.Read(bornHostAddress)

	var storeTimestamp int64
	binary.Read(byteBuff, binary.BigEndian, &storeTimestamp)

	storeHostAddress := make([]byte, 8)
	byteBuff.Read(storeHostAddress)

	var reconsumeTimes int32
	binary.Read(byteBuff, binary.BigEndian, &reconsumeTimes)

	var preparedTransactionOffset int64
	binary.Read(byteBuff, binary.BigEndian, &preparedTransactionOffset)

	var bodyLen int32
	binary.Read(byteBuff, binary.BigEndian, &bodyLen)

	if bodyLen > 0 {
		var msgBody = make([]byte, bodyLen)
		readLength, err := byteBuff.Read(msgBody)
		if err != nil {
			log.Errorf("read body error : %s", err.Error())
			return &DispatchRequest{
				msgSize: 0,
				success: false,
			}
		}

		if int(bodyLen) != readLength {
			log.Errorf("msg body length: %d, read length: %d", bodyLen, readLength)
			return &DispatchRequest{
				msgSize: 0,
				success: false,
			}
		}

	}

	topicLen, _ := byteBuff.ReadByte()
	topic := make([]byte, topicLen)
	byteBuff.Read(topic)

	var propertiesLength int16
	propertiesMap := map[string]string{}
	binary.Read(byteBuff, binary.BigEndian, &propertiesLength)
	if propertiesLength > 0 {
		properties := make([]byte, propertiesLength)
		byteBuff.Read(properties)
	}

	readLength := calMsgLength(int(bodyLen), int(topicLen), int(propertiesLength))
	if int(totalSize) != readLength {
		log.Errorf("totalSize: %d, readLength: %d, bodyLen: %d, topicLen: %d ,propertiesLength: %d", totalSize, readLength, bodyLen, topicLen, propertiesLength)
		return &DispatchRequest{
			msgSize: 0,
			success: false,
		}
	}

	return &DispatchRequest{
		topic:                     string(topic),
		queueId:                   queueId,
		commitLogOffset:           physicOffset,
		msgSize:                   totalSize,
		tagsCode:                  0,
		storeTimestamp:            storeTimestamp,
		keys:                      "",
		success:                   true,
		uniqKey:                   "",
		sysFlag:                   sysFlag,
		preparedTransactionOffset: preparedTransactionOffset,
		propertiesMap:             propertiesMap,
	}

}

func (r CommitLog) GetMessage(offset int64, size int32) *SelectMappedBufferResult {
	mappedFile := r.mappedFileQueue.findMappedFileByOffset(offset, offset == 0)
	if mappedFile == nil {
		return nil
	}

	pos := offset % commitLogFileSize
	return mappedFile.selectMappedBufferBySize(int32(pos), size)
}

func (r CommitLog) RollNextFile(offset int64) int64 {
	return offset + commitLogFileSize - offset%commitLogFileSize
}

type DefaultAppendMessageCallback struct {
	msgIdMemory bytes.Buffer

	msgStoreItemMemory bytes.Buffer
	maxMessageSize     int32
	keyBuilder         strings.Builder
	msgIdBuilder       strings.Builder
}

func (r *DefaultAppendMessageCallback) DoAppend(fileMap mmap.MMap, currentOffset int32, fileFromOffset int64, maxBlank int32, ext *MessageExtBrokerInner) *AppendMessageResult {
	log.Infof("fileFromOffset %d DoAppend OK", fileFromOffset)

	wroteOffset := fileFromOffset + int64(currentOffset)

	r.keyBuilder.Reset()
	r.msgIdBuilder.Reset()
	msgStoreItemMemory := r.msgStoreItemMemory
	msgStoreItemMemory.Reset()

	r.keyBuilder.WriteString(ext.Topic)
	r.keyBuilder.WriteString("-")
	r.keyBuilder.WriteString(strconv.Itoa(int(ext.QueueId)))
	key := r.keyBuilder.String()

	var queueOffset int64
	if tmpQueueOffset, ok := topicQueueTable[key]; ok {
		queueOffset = tmpQueueOffset
	} else {
		topicQueueTable[key] = 0
	}

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
	msgStoreItemMemory.Write(util.Int32ToBytes(0))
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

	appendResult := &AppendMessageResult{
		WroteBytes:   int32(msgLength),
		WroteOffset:  wroteOffset,
		Status:       AppendOk,
		LogicsOffset: queueOffset,
	}

	// next offset
	topicQueueTable[key] = queueOffset + 1
	return appendResult
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
