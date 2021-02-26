package broker

import (
	"nqs/remoting"
	net2 "nqs/remoting/channel"
	"nqs/remoting/protocol"
	"nqs/store"
)

type BrokerController struct {
	Server *DefaultServer
	Store  *store.DefaultMessageStore
}

func Initialize() *BrokerController {
	b := &BrokerController{}
	defaultServer := &DefaultServer{
		ChannelMap:  map[string]*net2.Channel{},
		Encoder:     &protocol.JsonEncoder{},
		Decoder:     &protocol.JsonDecoder{},
		ResponseMap: map[int32]*remoting.ResponseFuture{},
	}

	b.Store = store.NewStore()
	b.Store.Start()

	load := b.Store.Load()
	if !load {
		panic("store load 失败")
	}

	b.Server = defaultServer

	defaultServer.Start(b)

	return b
}

func (r BrokerController) Shutdown() {
	r.Store.Shutdown()
}
