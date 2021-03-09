package processor

import (
	log "github.com/sirupsen/logrus"
	"nqs/code"
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
	"nqs/store"
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

}

func (r *ConsumerManageProcessor) queryConsumerOffset(command *protocol.Command, channel *channel.Channel) {

}
