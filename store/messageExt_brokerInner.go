package store

import (
	"nqs/common/message"
	"sync"
)

var extPool = &sync.Pool{
	New: func() interface{} {
		return &MessageExtBrokerInner{}
	}}

type MessageExtBrokerInner struct {
	message.MessageExt
	propertiesString string
	tagsCode         int64
}

func (ext *MessageExtBrokerInner) GetBody() []byte {
	return ext.Message.Body
}

func GetExtMessage() *MessageExtBrokerInner {
	return extPool.Get().(*MessageExtBrokerInner)
}

func BackExtMessage(ext *MessageExtBrokerInner) {
	ext.Body = nil
	ext.QueueId = 0
	ext.CommitLogOffset = 0
	ext.QueueOffset = 0
	ext.MsgId = ""
	ext.BornTimestamp = 0
	ext.SysFlag = 0
	ext.StoreSize = 0

	extPool.Put(ext)
}
