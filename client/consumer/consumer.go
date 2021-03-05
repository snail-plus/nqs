package consumer

import (
	"fmt"
	"nqs/client"
	"nqs/common/message"
	"sync"
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

type PullRequest struct {
	consumerGroup string
	mq            message.MessageQueue
	nextOffset    int64
	lockedFirst   bool
}

func (pr *PullRequest) String() string {
	return fmt.Sprintf("[ConsumerGroup: %s, Topic: %s, MessageQueue: %d]",
		pr.consumerGroup, pr.mq.Topic, pr.mq.QueueId)
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
}

func NewPushConsumer(group string) (*PushConsumer, error) {
	dc := &defaultConsumer{consumerGroup: group, client: client.GetOrNewRocketMQClient("sss")}
	p := &PushConsumer{defaultConsumer: dc}
	return p, nil
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
	return nil
}
