package broker

import (
	"github.com/henrylee2cn/goutil/calendar/cron"
	log "github.com/sirupsen/logrus"
	"nqs/remoting"
	net2 "nqs/remoting/channel"
	"nqs/remoting/protocol"
	"nqs/store"
)

type BrokerController struct {
	Server                *DefaultServer
	Store                 *store.DefaultMessageStore
	ConsumerOffsetManager *ConsumerOffsetManager

	cron *cron.Cron
}

func Initialize() *BrokerController {
	b := &BrokerController{}
	defaultServer := &DefaultServer{
		ChannelMap:  map[string]*net2.Channel{},
		Encoder:     &protocol.JsonEncoder{},
		Decoder:     &protocol.JsonDecoder{},
		ResponseMap: map[int32]*remoting.ResponseFuture{},
	}

	// 设置controller
	b.Server = defaultServer
	b.ConsumerOffsetManager = NewConsumerOffsetManager()
	b.cron = cron.New()

	b.Store = store.NewStore()
	loadOk := b.Store.Load()

	loadOk = loadOk && b.ConsumerOffsetManager.Load()

	if !loadOk {
		panic("store load 失败")
	}

	b.Store.Start()
	b.cron.Start()
	b.startTask()

	defaultServer.Start(b)

	return b
}

func (r BrokerController) Shutdown() {
	r.Store.Shutdown()
}

func (r BrokerController) startTask() {

	r.cron.AddFunc("*/10 * * * * ?", func() {
		r.ConsumerOffsetManager.Persist()
		log.Infof("persist consumerOffset")
	})

}
