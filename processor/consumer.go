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
	log.Debugf(" 收到请求：%+v", command)

	switch command.Code {
	case code.QueryConsumerOffset:
		r.queryConsumerOffset(command, channel)
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
	consumerOffsetManager := r.Store.ConsumerOffsetManager.Config.(*store.ConsumerOffsetManager)
	consumerOffsetManager.CommitOffset(clientHost, requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId, requestHeader.CommitOffset)

	responseHeader := message.UpdateConsumerOffsetResponseHeader{}
	response := command.CreateResponseCommand()
	response.Code = code.Success
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

	consumerOffsetManager := r.Store.ConsumerOffsetManager.Config.(*store.ConsumerOffsetManager)
	offset := consumerOffsetManager.QueryOffset(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId)
	log.Debugf("group: %s, topic: %s, queueId: %d, offset: %d", requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId, offset)
	responseHeader := message.QueryConsumerOffsetResponseHeader{Offset: offset}
	response := command.CreateResponseCommand()
	response.CustomHeader = responseHeader
	channel.WriteToConn(response)
}
