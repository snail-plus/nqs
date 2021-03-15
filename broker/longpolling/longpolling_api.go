package longpolling

import (
	ch "nqs/remoting/channel"
	"nqs/remoting/protocol"
)

type PullRequest struct {
	ClientChannel      *ch.Channel
	PullFromThisOffset int64
	RequestCommand     *protocol.Command

	TimeoutMillis    int64
	SuspendTimestamp int64
}

type LongPolling interface {
	Start()
	NotifyMessageArriving(topic string, queueId int, maxOffset int64)
	SuspendPullRequest(topic string, queueId int32, pullRequest *PullRequest)
}

type NotifyMessageArrivingListener struct {
	LongPolling LongPolling
}

func (r *NotifyMessageArrivingListener) Arriving(topic string, queueId int32, logicOffset int64) {
	// log.Infof("有新消息存储 offset: %d 调用NotifyMessageArriving", logicOffset)
	r.LongPolling.NotifyMessageArriving(topic, int(queueId), logicOffset)
}
