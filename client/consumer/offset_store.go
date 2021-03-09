package consumer

import (
	log "github.com/sirupsen/logrus"
	"nqs/client"
	"nqs/code"
	"nqs/common/message"
	"nqs/remoting/protocol"
	"sync"
)

type OffsetStore interface {
	persist(mqs []*message.MessageQueue)
	update(mq *message.MessageQueue, offset int64, increaseOnly bool)
}

type RemoteBrokerOffsetStore struct {
	mutex       sync.RWMutex
	OffsetTable map[message.MessageQueue]int64 `json:"OffsetTable"`
	group       string
	mqClient    *client.RMQClient
	namesrv     client.Namesrvs
}

func NewRemoteOffsetStore(group string, client *client.RMQClient, namesrv client.Namesrvs) OffsetStore {
	return &RemoteBrokerOffsetStore{
		OffsetTable: make(map[message.MessageQueue]int64),
		group:       group,
		mqClient:    client,
		namesrv:     namesrv,
	}
}

func (r *RemoteBrokerOffsetStore) update(mq *message.MessageQueue, offset int64, increaseOnly bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	localOffset, exist := r.OffsetTable[*mq]
	if !exist {
		r.OffsetTable[*mq] = offset
		return
	}

	if increaseOnly {
		if localOffset < offset {
			r.OffsetTable[*mq] = offset
		}
	} else {
		r.OffsetTable[*mq] = offset
	}
}

func (r *RemoteBrokerOffsetStore) persist(mqs []*message.MessageQueue) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if len(mqs) == 0 {
		return
	}

	used := make(map[message.MessageQueue]struct{}, 0)
	for _, mq := range mqs {
		used[*mq] = struct{}{}
	}

	for mq, off := range r.OffsetTable {
		if _, ok := used[mq]; !ok {
			delete(r.OffsetTable, mq)
			continue
		}

		err := r.updateConsumeOffsetToBroker(r.group, mq, off)
		if err != nil {
			log.Errorf("updateConsumeOffsetToBroker error: %s", err.Error())
			continue
		}

	}
}

func (r *RemoteBrokerOffsetStore) updateConsumeOffsetToBroker(group string, mq message.MessageQueue, off int64) error {
	addr := r.namesrv.FindBrokerAddrByName(mq.BrokerName)
	header := message.UpdateConsumerOffsetRequestHeader{
		ConsumerGroup: group,
		Topic:         mq.Topic,
		QueueId:       int32(mq.QueueId),
		CommitOffset:  off,
	}

	command := protocol.CreatesRequestCommand()
	command.Code = code.UpdateConsumerOffset
	command.CustomHeader = header

	err := r.mqClient.RemoteClient.InvokeOneWay(addr, command, 3000)
	if err != nil {
		return err
	}

	return err
}
