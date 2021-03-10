package broker

import (
	"github.com/henrylee2cn/goutil/calendar/cron"
	log "github.com/sirupsen/logrus"
	"nqs/common"
	"nqs/store"
)

type BrokerController struct {
	Server                *DefaultServer
	Store                 *store.DefaultMessageStore
	ConsumerOffsetManager *common.ConfigManager

	cron *cron.Cron
}

func Initialize() *BrokerController {
	b := &BrokerController{}
	defaultServer := NewDefaultServer()

	// 设置controller
	b.Server = defaultServer
	b.ConsumerOffsetManager = store.NewConsumerOffsetManager()
	b.cron = cron.New()

	b.Store = store.NewStore()
	b.Store.ConsumerOffsetManager = b.ConsumerOffsetManager
	loadOk := b.Store.Load()

	loadOk = loadOk && b.ConsumerOffsetManager.Load()

	if !loadOk {
		panic("store load 失败")
	}

	b.Store.Start()
	b.cron.Start()
	b.startTask()

	defaultServer.Start(b)

	log.Info("Broker boot success")
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
