package processor

import (
	log "github.com/sirupsen/logrus"
	"nqs/common/message"
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
	"nqs/store"
	"nqs/util"
)

type PullMessageProcessor struct {
	Name string
	Store *store.DefaultMessageStore
}

func (s *PullMessageProcessor) Reject() bool {
	return false
}

func (s *PullMessageProcessor) ProcessRequest(request *protocol.Command, channel *channel.Channel)  {

	requestHeader := message.PullMessageRequestHeader{}
	err := util.MapToStruct(request.ExtFields, &requestHeader)
	if err != nil {
		log.Error("MapToStruct error: " + err.Error())
		return
	}

	getMessage := s.Store.GetMessage(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueOffset, requestHeader.MaxMsgNums)
	if getMessage.Status == store.Found {

	}
}
