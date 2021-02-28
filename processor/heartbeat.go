package processor

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"nqs/common/protocol/heartbeat"
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
)

type HeartbeatProcessor struct {
	Name string
}

func (s *HeartbeatProcessor) Reject() bool {
	return false
}

func (s *HeartbeatProcessor) ProcessRequest(request *protocol.Command, channel *channel.Channel) {
	if request.Body == nil {
		return
	}

	heartbeatData := heartbeat.Heartbeat{}
	err := json.Unmarshal(request.Body, &heartbeatData)
	if err != nil {
		log.Error("heartbeat unmarshal error: " + err.Error() + ", body: " + string(request.Body))
		return
	}

	log.Debugf("receive heartbeat, body: %+v", heartbeatData)

	responseCommand := request.CreateResponseCommand()
	channel.WriteCommand(responseCommand)

}
