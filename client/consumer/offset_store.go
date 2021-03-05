package consumer

import "nqs/common/message"

type OffsetStore interface {
	persist(mqs []*message.MessageQueue)
}

type remoteBrokerOffsetStore struct {
}

func (r *remoteBrokerOffsetStore) persist(mqs []*message.MessageQueue) {

}
