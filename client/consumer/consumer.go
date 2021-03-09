package consumer

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"nqs/client"
	"nqs/client/inner"
	"nqs/common/message"
	"sync"
	"time"
)

type MessageModel int

const (
	BroadCasting MessageModel = iota
	Clustering
)

func (mode MessageModel) String() string {
	switch mode {
	case BroadCasting:
		return "BroadCasting"
	case Clustering:
		return "Clustering"
	default:
		return "Unknown"
	}
}

type ConsumeFromWhere int

const (
	ConsumeFromLastOffset ConsumeFromWhere = iota
	ConsumeFromFirstOffset
	ConsumeFromTimestamp
)

type ExpressionType string

const (
	SQL92 = ExpressionType("SQL92")
	TAG   = ExpressionType("TAG")
)

func IsTagType(exp string) bool {
	if exp == "" || exp == "TAG" {
		return true
	}
	return false
}

type MessageSelector struct {
	Type       ExpressionType
	Expression string
}

type ConsumeResult int

const (
	ConsumeSuccess ConsumeResult = iota
	ConsumeRetryLater
	Commit
	Rollback
	SuspendCurrentQueueAMoment
)

type ConsumeResultHolder struct {
	ConsumeResult
}

type ConsumerReturn int

const (
	SuccessReturn ConsumerReturn = iota
	ExceptionReturn
	NullReturn
	TimeoutReturn
	FailedReturn
)

type ProcessQueue struct {
	MsgCh chan []*message.MessageExt
}

type PullRequest struct {
	ConsumerGroup string
	Mq            *message.MessageQueue
	NextOffset    int64
	LockedFirst   bool
	Pq            *ProcessQueue
}

func (pr *PullRequest) String() string {
	return fmt.Sprintf("[ConsumerGroup: %s, Topic: %s, MessageQueue: %d]",
		pr.ConsumerGroup, pr.Mq.Topic, pr.Mq.QueueId)
}

type ConsumeType string

const (
	_PullConsume = ConsumeType("CONSUME_ACTIVELY")
	_PushConsume = ConsumeType("CONSUME_PASSIVELY")

	_SubAll = "*"
)

type defaultConsumer struct {
	consumerGroup          string
	model                  MessageModel
	allocate               func(string, string, []*message.MessageQueue, []string) []*message.MessageQueue
	unitMode               bool
	consumeOrderly         bool
	fromWHere              ConsumeFromWhere
	consumerStartTimestamp int64
	consumeType            ConsumeType
	msgCHanged             func(topic string, mqAll, mqDivided []*message.MessageQueue)
	state                  int32
	pause                  bool
	once                   sync.Once
	prCh                   chan PullRequest
	pullFromWhichNodeTable sync.Map

	storage           OffsetStore
	processQueueTable sync.Map

	client *client.RMQClient

	namesrv client.Namesrvs
}

func (dc *defaultConsumer) start() error {
	dc.client.Start()
	return nil
}

func (dc *defaultConsumer) persistConsumerOffset() error {
	mqs := make([]*message.MessageQueue, 0)
	dc.processQueueTable.Range(func(key, value interface{}) bool {
		k := key.(message.MessageQueue)
		mqs = append(mqs, &k)
		return true
	})

	dc.storage.persist(mqs)
	return nil
}

type PushConsumer struct {
	*defaultConsumer
	ConsumeMsg func(ext []*message.MessageExt)
	done       chan struct{}
}

func NewPushConsumer(group, topic string) (*PushConsumer, error) {
	mqClient := client.GetOrNewRocketMQClient("sss")
	dc := &defaultConsumer{
		consumerGroup: group,
		client:        mqClient,
		prCh:          make(chan PullRequest, 4),
		storage:       NewRemoteOffsetStore(group, mqClient, client.Namesrvs{}),
	}

	mq := message.MessageQueue{Topic: topic, QueueId: 1}
	pq := ProcessQueue{MsgCh: make(chan []*message.MessageExt, 10)}

	dc.prCh <- PullRequest{
		ConsumerGroup: group,
		Mq:            &mq,
		NextOffset:    0,
		LockedFirst:   true,
		Pq:            &pq,
	}

	dc.processQueueTable.Store(mq, pq)

	pc := &PushConsumer{defaultConsumer: dc}
	return pc, nil
}

func (pc *PushConsumer) PersistConsumerOffset() error {
	return pc.defaultConsumer.persistConsumerOffset()
}

// PushConsumer start -> defaultConsumer start -> client start
func (pc *PushConsumer) Start() error {
	err := pc.client.RegisterConsumer(pc.consumerGroup, pc)
	if err != nil {
		return err
	}

	pc.defaultConsumer.start()

	go func() {
		// todo start clean msg expired
		for {
			select {
			case pr := <-pc.prCh:
				go func() {
					pc.pullMessage(&pr)
				}()
			case <-pc.done:
				return
			}
		}
	}()

	return nil
}

func (pc *PushConsumer) pullMessage(request *PullRequest) {
	go func() {
		// 消息消息
		for {
			select {
			case <-pc.done:
				println("")
			default:
				pq := request.Pq
				exts := <-pq.MsgCh
				if pc.ConsumeMsg != nil {
					pc.ConsumeMsg(exts)
				}
			}
		}
	}()

	for {
		select {
		case <-pc.done:
			log.Infof("push consumer close message handle.")
			return
		default:
		}

		// TODO 从name server 获取
		addr := "localhost:8089"
		for {
			pullResult, err := pc.client.RemoteClient.PullMessage(addr, request.Mq.Topic, request.NextOffset, int32(request.Mq.QueueId), 23)
			if err != nil {
				log.Errorf("err: %s", err.Error())
				continue
			}

			switch pullResult.PullStatus {
			case inner.Found:
				msgFoundList := pullResult.MsgFoundList
				if msgFoundList.Len() == 0 {
					time.Sleep(1 * time.Second)
					continue
				}

				log.Debugf("查到消息条数：%d, NextBeginOffset: %d", msgFoundList.Len(), pullResult.NextBeginOffset)
				msgList := make([]*message.MessageExt, 0)
				for item := msgFoundList.Front(); item != nil; item = item.Next() {
					msgList = append(msgList, item.Value.(*message.MessageExt))
				}
				request.Pq.MsgCh <- msgList

				request.NextOffset = pullResult.NextBeginOffset
				// TODO 更新Offset
				pc.storage.update(request.Mq, request.NextOffset, false)
			case inner.NoNewMsg:
				time.Sleep(100 * time.Millisecond)
			default:
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
	}
}
