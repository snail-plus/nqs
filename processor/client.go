package processor

import (
	log "github.com/sirupsen/logrus"
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
)

type ClientProcessor struct {
	Name string
}

func (r *ClientProcessor) Reject() bool {
	return false
}

func (r *ClientProcessor) ProcessRequest(command *protocol.Command, channel *channel.Channel)  {
	log.Infof(" 收到请求：%+v", command)
}
