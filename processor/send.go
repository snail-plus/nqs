package processor

import (
	log "github.com/sirupsen/logrus"
	"nqs/code"
	"nqs/common/message"
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
	"nqs/store"
	"nqs/util"
)

type SendMessageProcessor struct {
	Name  string
	Store *store.DefaultMessageStore
}

func (s *SendMessageProcessor) Reject() bool {
	return false
}

func (s *SendMessageProcessor) ProcessRequest(request *protocol.Command, channel *channel.Channel) {

	sendMessageRequestHeader := message.SendMessageRequestHeader{}
	err := util.MapToStruct(request.ExtFields, &sendMessageRequestHeader)

	if err != nil {
		log.Error("MapToStruct error: " + err.Error())
		return
	}

	inner := &store.MessageExtBrokerInner{}

	inner.QueueId = sendMessageRequestHeader.QueueId
	inner.Topic = sendMessageRequestHeader.Topic
	inner.BornTimestamp = sendMessageRequestHeader.BornTimestamp

	inner.BrokerName = "M1"
	inner.Body = request.Body
	inner.BornHost = channel.Conn.RemoteAddr().String()
	inner.StoreHost = channel.Conn.LocalAddr().String()

	putResult := s.Store.PutMessages(inner)
	response := request.CreateResponseCommand()
	s.handlePutMessageResult(putResult, response, channel)
}

func (s *SendMessageProcessor) handlePutMessageResult(putResult *store.PutMessageResult, response *protocol.Command,
	channel *channel.Channel) {
	if putResult.PutMessageStatus == store.PutOk {
		appendResult := putResult.AppendMessageResult
		offset := appendResult.LogicsOffset
		responseHeader := message.SendMessageResponseHeader{QueueOffset: offset, MsgId: putResult.AppendMessageResult.MsgId}
		response.CustomHeader = responseHeader
		response.Code = code.Success
	} else {
		response.Code = code.SystemError
	}

	channel.WriteCommand(response)
}
