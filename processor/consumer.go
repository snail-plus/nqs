package processor

import (
	log "github.com/sirupsen/logrus"
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
}
