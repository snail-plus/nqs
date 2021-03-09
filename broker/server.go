package broker

import (
	"errors"
	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"net"
	"nqs/code"
	"nqs/processor"
	"nqs/remoting"
	net2 "nqs/remoting/channel"
	"nqs/remoting/protocol"
	"sync"
	"time"
)

type DefaultServer struct {
	lock        sync.Mutex
	ChannelMap  sync.Map //map[string]*net2.Channel
	Encoder     protocol.Encoder
	Decoder     protocol.Decoder
	ResponseMap map[int32]*remoting.ResponseFuture
	listener    net.Listener
}

func (r *DefaultServer) registerProcessor(b *BrokerController) {
	sendMsgPool, _ := ants.NewPool(1, ants.WithPreAlloc(true), ants.WithMaxBlockingTasks(10000))
	defaultPool, _ := ants.NewPool(8, ants.WithPreAlloc(true), ants.WithMaxBlockingTasks(10000))

	processor.PMap[code.Heartbeat] = processor.Pair{Pool: defaultPool, Processor: &processor.HeartbeatProcessor{Name: "Heartbeat"}}
	processor.PMap[code.SendMessage] = processor.Pair{Pool: sendMsgPool, Processor: &processor.SendMessageProcessor{Name: "Send", Store: b.Store}}
	processor.PMap[code.PullMessage] = processor.Pair{Pool: defaultPool, Processor: &processor.PullMessageProcessor{Name: "Pull", Store: b.Store}}

	processor.PMap[code.QueryConsumerOffset] = processor.Pair{Pool: defaultPool, Processor: &processor.ConsumerManageProcessor{Name: "Consumer", Store: b.Store}}
	processor.PMap[code.UpdateConsumerOffset] = processor.Pair{Pool: defaultPool, Processor: &processor.ConsumerManageProcessor{Name: "Consumer", Store: b.Store}}

}

func (r *DefaultServer) InvokeOneWay(addr string, command *protocol.Command, timeoutMillis int64) error {
	return nil
}

func (r *DefaultServer) InvokeSync(addr string, command *protocol.Command, timeoutMillis int64) (*protocol.Command, error) {
	load, ok := r.ChannelMap.Load(addr)
	if !ok {
		log.Errorf("addr: %s", addr)
		return nil, errors.New("失败")
	}

	channel := load.(*net2.Channel)

	r.ResponseMap[command.Opaque] = &remoting.ResponseFuture{
		Opaque:         command.Opaque,
		Conn:           channel.Conn,
		BeginTimestamp: time.Now().Unix(),
		TimeoutMillis:  timeoutMillis,
		DoneChan:       make(chan bool),
	}

	err := channel.WriteCommand(command)
	if err != nil {
		return nil, err
	}

	response, err := r.ResponseMap[command.Opaque].WaitResponse(timeoutMillis)
	return response, err
}

func (r *DefaultServer) InvokeAsync(addr string, command *protocol.Command, timeoutMillis int64, invokeCallback func(*protocol.Command, error)) {
}

func (r *DefaultServer) AddChannel(addr string, conn net.Conn) *net2.Channel {

	channel := &net2.Channel{
		Encoder:   r.Encoder,
		Decoder:   r.Decoder,
		Conn:      conn,
		WriteChan: make(chan *protocol.Command, 2000),
	}

	r.ChannelMap.Store(addr, channel)
	return channel
}

func (r *DefaultServer) Start(b *BrokerController) {

	r.registerProcessor(b)

	listen, err := net.Listen("tcp4", "127.0.0.1:8089")
	if err != nil {
		panic(err)
	}

	r.listener = listen

	go func() {
		for {
			conn, err := listen.Accept()
			if err != nil {
				break
			}

			channel := r.AddChannel(conn.RemoteAddr().String(), conn)
			go r.handleRead(channel)
			go r.handleWrite(channel)
		}
	}()

}

func (r *DefaultServer) handleRead(channel *net2.Channel) {

	defer func() {
		channel.Closed = true
		close(channel.WriteChan)
		r.ChannelMap.Delete(channel.Conn.RemoteAddr().String())
	}()

	remoting.ReadMessage(channel)

}

func (r *DefaultServer) handleWrite(channel *net2.Channel) {
	for !channel.Closed {
		select {
		case response, isOpen := <-channel.WriteChan:
			if !isOpen {
				break
			}

			err := channel.WriteToConn(response)
			if err != nil {
				continue
			}
		}
	}
}

func (r *DefaultServer) Shutdown() {
	r.listener.Close()
	log.Infof("Shutdown tcp server")
}
