package client

import (
	"fmt"
	"github.com/henrylee2cn/goutil/calendar/cron"
	"nqs/remoting"
	"sync"
)

type RMQClient struct {
	remoteClient *remoting.DefaultClient

	producerMap sync.Map

	consumerMap sync.Map

	close bool

	done chan struct{}

	cron *cron.Cron
}

var clientMap sync.Map

func GetOrNewRocketMQClient(clientId string) *RMQClient {
	actual, loaded := clientMap.Load(clientId)
	if !loaded {
		return &RMQClient{
			remoteClient: remoting.CreateClient(),
			cron:         cron.New(),
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

	r.cron.AddFunc("*/10 * * * * ?", func() {
		r.consumerMap.Range(func(key, value interface{}) bool {
			consumer := value.(InnerConsumer)
			consumer.PersistConsumerOffset()
			return true
		})
	})

}

func (r *RMQClient) Shutdown() {
	r.cron.Stop()
	r.close = true
}
