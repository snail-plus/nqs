package broker

import (
	"context"
	"errors"
	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"net"
	"nqs/code"
	"nqs/processor"
	"nqs/remoting"
	ch "nqs/remoting/channel"
	"nqs/remoting/protocol"
	"sync"
	"time"
)

type DefaultServer struct {
	remoting.Remoting
	lock     sync.Mutex
	listener net.Listener
}

func NewDefaultServer() *DefaultServer {
	server := &DefaultServer{
		Remoting: remoting.Remoting{
			ResponseTable:   sync.Map{},
			ConnectionTable: sync.Map{},
		},
		lock: sync.Mutex{},
	}
	return server
}

func (r *DefaultServer) registerProcessor(b *BrokerController) {
	sendMsgPool, _ := ants.NewPool(1, ants.WithPreAlloc(true), ants.WithMaxBlockingTasks(10000))
	defaultPool, _ := ants.NewPool(8, ants.WithPreAlloc(true), ants.WithMaxBlockingTasks(10000))

	processor.PMap[code.Heartbeat] = processor.Pair{Pool: defaultPool, Processor: &processor.HeartbeatProcessor{Name: "Heartbeat"}}
	processor.PMap[code.SendMessage] = processor.Pair{Pool: sendMsgPool, Processor: &processor.SendMessageProcessor{Name: "Send", Store: b.Store}}
	processor.PMap[code.PullMessage] = processor.Pair{Pool: defaultPool, Processor: &processor.PullMessageProcessor{Name: "Pull", Store: b.Store, PullRequestHoldService: b.PullRequestHoldService}}

	processor.PMap[code.QueryConsumerOffset] = processor.Pair{Pool: defaultPool, Processor: &processor.ConsumerManageProcessor{Name: "Consumer", Store: b.Store}}
	processor.PMap[code.UpdateConsumerOffset] = processor.Pair{Pool: defaultPool, Processor: &processor.ConsumerManageProcessor{Name: "Consumer", Store: b.Store}}

}

func (r *DefaultServer) InvokeOneWay(ctx context.Context, addr string, command *protocol.Command) error {
	load, ok := r.Remoting.ConnectionTable.Load(addr)
	if !ok {
		log.Errorf("addr: %s", addr)
		return errors.New("失败")
	}

	channel := load.(*ch.Channel)

	return channel.WriteCommand(command)
}

func (r *DefaultServer) InvokeSync(ctx context.Context, addr string, command *protocol.Command) (*protocol.Command, error) {
	load, ok := r.Remoting.ConnectionTable.Load(addr)
	if !ok {
		log.Errorf("addr: %s", addr)
		return nil, errors.New("失败")
	}

	channel := load.(*ch.Channel)

	future := &remoting.ResponseFuture{
		Opaque:         command.Opaque,
		Conn:           channel.Conn,
		BeginTimestamp: time.Now().Unix(),
		Ctx:            ctx,
		DoneChan:       make(chan bool),
	}

	r.ResponseTable.Store(command.Opaque, future)
	err := channel.WriteCommand(command)
	if err != nil {
		return nil, err
	}

	response, err := future.WaitResponse()
	return response, err
}

func (r *DefaultServer) InvokeAsync(ctx context.Context, addr string, command *protocol.Command, invokeCallback func(*protocol.Command, error)) error {
	return nil
}

func (r *DefaultServer) Start(b *BrokerController) {
	log.Infof("Start TCP Server")
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

func (r *DefaultServer) handleRead(channel *ch.Channel) {
	r.ReadMessage(channel)
}

func (r *DefaultServer) handleWrite(channel *ch.Channel) {
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

	r.ResponseTable.Range(func(key, value interface{}) bool {
		r.ResponseTable.Delete(key)
		return true
	})

	log.Infof("Shutdown tcp server")
}
