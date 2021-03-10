package client

import (
	"fmt"
	"github.com/henrylee2cn/goutil/calendar/cron"
	log "github.com/sirupsen/logrus"
	"nqs/remoting"
	"sync"
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
		heartbeat, err := r.RemoteClient.SendHeartbeat(addr)
		if err != nil {
			log.Errorf("%v", err)
			return
		}

		log.Infof("心跳返回: %v", heartbeat)
	})

}

func (r *RMQClient) Shutdown() {
	r.cron.Stop()
	r.close = true
}
