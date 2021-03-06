package consumer

import "nqs/common/message"

type OffsetStore interface {
	persist(mqs []*message.MessageQueue)
}

type RemoteBrokerOffsetStore struct {
}

func (r *RemoteBrokerOffsetStore) persist(mqs []*message.MessageQueue) {

}
