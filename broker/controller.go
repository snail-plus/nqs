package broker

import (
	"github.com/henrylee2cn/goutil/calendar/cron"
	"nqs/remoting"
	"nqs/remoting/protocol"
	"nqs/store"
	"sync"
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
		ChannelMap:  sync.Map{},
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
	r.Server.Shutdown()
	r.Store.Shutdown()
}

func (r BrokerController) startTask() {

	r.cron.AddFunc("*/10 * * * * ?", func() {
		r.ConsumerOffsetManager.Persist()
	})

}
