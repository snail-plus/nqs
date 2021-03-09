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

type ConsumerManageProcessor struct {
	Name  string
	Store *store.DefaultMessageStore
}

func (r *ConsumerManageProcessor) Reject() bool {
	return false
}

func (r *ConsumerManageProcessor) ProcessRequest(command *protocol.Command, channel *channel.Channel) {
	log.Infof(" 收到请求：%+v", command)

	switch command.Code {
	case code.QueryConsumerOffset:
		r.updateConsumerOffset(command, channel)
		break
	case code.UpdateConsumerOffset:
		r.updateConsumerOffset(command, channel)
		break
	default:
		return
	}
}

func (r *ConsumerManageProcessor) updateConsumerOffset(command *protocol.Command, channel *channel.Channel) {
	requestHeader := message.UpdateConsumerOffsetRequestHeader{}
	err := util.MapToStruct(command.ExtFields, &requestHeader)

	if err != nil {
		log.Error("MapToStruct error: " + err.Error())
		return
	}

	clientHost := channel.Conn.RemoteAddr().String()
	r.Store.ConsumerOffsetManager.CommitOffset(clientHost, requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId, requestHeader.CommitOffset)

	responseHeader := message.UpdateConsumerOffsetResponseHeader{}
	response := command.CreateResponseCommand()
	response.CustomHeader = responseHeader

	channel.WriteCommand(response)
}

func (r *ConsumerManageProcessor) queryConsumerOffset(command *protocol.Command, channel *channel.Channel) {

	requestHeader := message.QueryConsumerOffsetRequestHeader{}
	err := util.MapToStruct(command.ExtFields, &requestHeader)

	if err != nil {
		log.Error("MapToStruct error: " + err.Error())
		return
	}

	offset := r.Store.ConsumerOffsetManager.QueryOffset(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId)
	log.Infof("group: %s, topic: %s, queueId: %d, offset: %d", requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId, offset)
	responseHeader := message.QueryConsumerOffsetResponseHeader{Offset: offset}
	response := command.CreateResponseCommand()
	response.CustomHeader = responseHeader
	channel.WriteCommand(response)

}
