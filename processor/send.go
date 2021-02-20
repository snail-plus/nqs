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
	Name string
	Store *store.DefaultMessageStore
}

func (s *SendMessageProcessor) Reject() bool {
	return false
}

func (s *SendMessageProcessor) ProcessRequest(request *protocol.Command, channel *channel.Channel)  {
	sendMessageRequestHeader := message.SendMessageRequestHeader{}
	err := util.MapToStruct(request.ExtFields, &sendMessageRequestHeader)

	if err != nil {
		log.Error("MapToStruct error: " + err.Error())
		return
	}

	log.Info("收到发送请求 body: ", string(request.Body))
	response := request.CreateResponseCommand()

	inner := &store.MessageExtBrokerInner{}
	inner.QueueId = sendMessageRequestHeader.QueueId
	inner.Topic = sendMessageRequestHeader.Topic
	inner.BrokerName = util.GetLocalAddress()
	inner.BornTimestamp = sendMessageRequestHeader.BornTimestamp
	inner.Body = request.Body

	putResult := s.Store.PutMessages(inner)
    s.handlePutMessageResult(putResult, response, channel)

}

func (s *SendMessageProcessor) handlePutMessageResult(putResult *store.PutMessageResult, response *protocol.Command,
	channel *channel.Channel) {
	if putResult.PutMessageStatus == store.PutOk {
		appendResult := putResult.AppendMessageResult
		offset := appendResult.LogicsOffset
		log.Infof("offset %d", offset)
		responseHeader := message.SendMessageResponseHeader{QueueOffset: offset, MsgId: ""}
		response.CustomHeader = responseHeader
		response.Code = code.Success
	}else {
		response.Code = code.SystemError
	}

	channel.WriteCommand(response)
}
