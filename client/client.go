package client

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"github.com/henrylee2cn/goutil/calendar/cron"
	log "github.com/sirupsen/logrus"
	"nqs/client/inner"
	"nqs/code"
	"nqs/common/message"
	"nqs/common/protocol/heartbeat"
	"nqs/remoting"
	"nqs/remoting/protocol"
	"nqs/store"
	"nqs/util"
	"os"
	"strconv"
	"sync"
	"time"
)

type RMQClient struct {
	RemoteClient *remoting.DefaultClient

	producerMap *sync.Map

	consumerMap *sync.Map

	close bool

	done chan struct{}

	cron *cron.Cron

	namesrv Namesrvs
}

var clientMap sync.Map

func GetOrNewRocketMQClient(clientId string, namesrvs Namesrvs) *RMQClient {
	actual, loaded := clientMap.Load(clientId)
	if !loaded {
		return &RMQClient{
			RemoteClient: remoting.CreateClient(),
			cron:         cron.New(),
			consumerMap:  &sync.Map{},
			producerMap:  &sync.Map{},
			namesrv:      namesrvs,
		}
	}

	return actual.(*RMQClient)
}

func (r *RMQClient) RegisterConsumer(group string, consumer InnerConsumer) error {
	_, exist := r.consumerMap.Load(group)
	if exist {
		return fmt.Errorf("the consumer group exist already")
	}
	r.consumerMap.Store(group, consumer)
	return nil
}

func (r *RMQClient) Start() {
	// schedule persist offset
	r.cron.Start()

	if r.consumerMap == nil {
		return
	}

	r.cron.AddFunc("*/10 * * * * ?", func() {
		r.consumerMap.Range(func(key, value interface{}) bool {
			if value == nil {
				return false
			}
			consumer := value.(InnerConsumer)
			consumer.PersistConsumerOffset()
			return true
		})
	})

	// send
	r.cron.AddFunc("*/20 * * * * ?", func() {
		addr := r.namesrv.FindBrokerAddrByName("")
		heartbeat, err := r.SendHeartbeat(addr)
		if err != nil {
			log.Errorf("%v", err)
			return
		}

		log.Infof("心跳返回: %v", heartbeat)
	})

}

func (r *RMQClient) PullMessage(addr, topic, group string, offset int64, queueId, maxMsgCount int32) (*inner.PullResult, error) {
	header := message.PullMessageRequestHeader{}
	header.Topic = topic
	header.QueueId = queueId
	header.MaxMsgNums = maxMsgCount
	header.QueueOffset = offset
	header.ConsumerGroup = group

	command := protocol.CreatesRequestCommand()
	command.Code = code.PullMessage
	command.CustomHeader = header

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	response, err := r.RemoteClient.InvokeSync(ctx, addr, command)
	if err != nil {
		return nil, err
	}

	// body := response.Body
	// decode body 哈哈
	responseHeader := message.PullMessageResponseHeader{}
	err = util.MapToStruct(response.ExtFields, &responseHeader)
	if err != nil {
		return nil, err
	}

	pullResult := &inner.PullResult{MsgFoundList: list.New(),
		NextBeginOffset: responseHeader.NextBeginOffset, MinOffset: responseHeader.MinOffset,
		MaxOffset: responseHeader.MaxOffset,
	}

	if response.Code != int32(store.Found) {
		pullResult.PullStatus = inner.NoNewMsg
		return pullResult, nil
	}

	// decode msg body
	bodyByteBuffer := bytes.NewBuffer(response.Body)
	for bodyByteBuffer.Len() > 0 {
		messageExt := message.DecodeMsg(bodyByteBuffer, false)
		pullResult.MsgFoundList.PushBack(messageExt)
	}

	return pullResult, nil
}

func (r *RMQClient) SendHeartbeat(addr string) (*protocol.Command, error) {
	heartbeatData := heartbeat.Heartbeat{ClientId: util.GetLocalAddress() + "@" + strconv.Itoa(os.Getpid())}
	body, err := json.Marshal(heartbeatData)
	if err != nil {
		return nil, err
	}

	command := protocol.CreatesRequestCommand()
	command.Code = code.Heartbeat
	command.Body = body

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	response, err := r.RemoteClient.InvokeSync(ctx, addr, command)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (r *RMQClient) Shutdown() {
	r.cron.Stop()
	r.close = true
}

func (r *RMQClient) RegisterProducer(group string, p InnerProducer) {

}

func (r *RMQClient) InvokeSync(ctx context.Context, addr string, command *protocol.Command) (*protocol.Command, error) {
	return r.RemoteClient.InvokeSync(ctx, addr, command)
}
