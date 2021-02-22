package store

import "nqs/common/message"

type MessageExtBrokerInner struct {
	message.MessageExt
	propertiesString string
	tagsCode         int64
}

func (ext *MessageExtBrokerInner) GetBody() []byte {
	return ext.Message.Body
}
