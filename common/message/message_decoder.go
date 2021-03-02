package message

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	log "github.com/sirupsen/logrus"
	"nqs/util"
)

const (
	messageMagicCode = -626843481
	phyPosPosition   = 4 + 4 + 4 + 4 + 4 + 8
	sysFlagPosition  = 4 + 4 + 4 + 4 + 4 + 8 + 8
)

func CreateMessageId(addr []byte, offset int64) string {
	buffer := bytes.NewBuffer(make([]byte, 8))
	buffer.Reset()
	buffer.Write(addr)
	binary.Write(buffer, binary.BigEndian, offset)
	return hex.EncodeToString(buffer.Bytes())
}

func DecodeMsg(byteBuffer *bytes.Buffer, isClient bool) *MessageExt {
	var ext = &MessageExt{}

	var storeSize int32
	binary.Read(byteBuffer, binary.BigEndian, &storeSize)
	ext.StoreSize = storeSize

	var magicCode int32
	binary.Read(byteBuffer, binary.BigEndian, &magicCode)

	var bodyCrc int32
	binary.Read(byteBuffer, binary.BigEndian, &bodyCrc)

	var queueId int32
	binary.Read(byteBuffer, binary.BigEndian, &queueId)
	ext.QueueId = queueId

	var flag int32
	binary.Read(byteBuffer, binary.BigEndian, &flag)
	ext.Flag = flag

	var queueOffset int64
	binary.Read(byteBuffer, binary.BigEndian, &queueOffset)
	ext.QueueOffset = queueOffset

	var physicOffset int64
	binary.Read(byteBuffer, binary.BigEndian, &physicOffset)
	ext.CommitLogOffset = physicOffset

	var sysFlag int32
	binary.Read(byteBuffer, binary.BigEndian, &sysFlag)
	ext.SysFlag = sysFlag

	var bornTimeStamp int64
	binary.Read(byteBuffer, binary.BigEndian, &bornTimeStamp)
	ext.BornTimestamp = bornTimeStamp

	var bornHost = make([]byte, 8)
	byteBuffer.Read(bornHost)
	ext.BornHost = util.ByteToAddress(bornHost)

	var storeTimeStamp int64
	binary.Read(byteBuffer, binary.BigEndian, &storeTimeStamp)
	ext.StoreTimestamp = storeTimeStamp

	var storeHost = make([]byte, 8)
	byteBuffer.Read(storeHost)
	ext.StoreHost = util.ByteToAddress(storeHost)

	var reconsumeTimes int32
	binary.Read(byteBuffer, binary.BigEndian, &reconsumeTimes)
	ext.ReconsumeTimes = reconsumeTimes

	var preparedTransactionOffset int64
	binary.Read(byteBuffer, binary.BigEndian, &preparedTransactionOffset)
	ext.PreparedTransactionOffset = preparedTransactionOffset

	var bodyLen int32
	binary.Read(byteBuffer, binary.BigEndian, &bodyLen)

	if bodyLen > 0 {
		msgBody := make([]byte, bodyLen)
		byteBuffer.Read(msgBody)
		ext.Body = msgBody
	}

	var topicLength int8
	binary.Read(byteBuffer, binary.BigEndian, &topicLength)
	topic := make([]byte, topicLength)
	byteBuffer.Read(topic)
	ext.Topic = string(topic)

	var propertiesLength int16
	binary.Read(byteBuffer, binary.BigEndian, &propertiesLength)
	if propertiesLength > 0 {
		properties := make([]byte, propertiesLength)
		byteBuffer.Read(properties)
	}

	msgId := CreateMessageId(storeHost, ext.CommitLogOffset)
	ext.MsgId = msgId

	log.Infof("decode msg success, storeSize: %d, bodyLength: %d, body: %s", storeSize, bodyLen, string(ext.Body))
	return ext
}
