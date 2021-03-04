package processor

import (
	log "github.com/sirupsen/logrus"
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
)

type AdminBrokerProcessor struct {
	Name string
}

func (r *AdminBrokerProcessor) Reject() bool {
	return false
}

func (r *AdminBrokerProcessor) ProcessRequest(command *protocol.Command, channel *channel.Channel) {
	log.Infof(" 收到请求：%+v", command)
}
