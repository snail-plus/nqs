package message

import (
	"fmt"
	"nqs/util"
)

type MessageQueue struct {
	Topic      string `json:"Topic"`
	BrokerName string `json:"BrokerName"`
	QueueId    int    `json:"QueueId"`
}

func (mq *MessageQueue) String() string {
	return fmt.Sprintf("MessageQueue [topic=%s, brokerName=%s, queueId=%d]", mq.Topic, mq.BrokerName, mq.QueueId)
}

func (mq *MessageQueue) HashCode() int {
	result := 1
	result = 31*result + util.HashString(mq.BrokerName)
	result = 31*result + mq.QueueId
	result = 31*result + util.HashString(mq.Topic)

	return result
}
