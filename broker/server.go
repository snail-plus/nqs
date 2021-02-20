package broker

import (
	"errors"
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
	lock sync.Mutex
	ChannelMap map[string]*net2.Channel
	Encoder protocol.Encoder
	Decoder protocol.Decoder
	ResponseMap map[int32]*remoting.ResponseFuture
}

func (r *DefaultServer) registerProcessor(b *BrokerController) {
	processor.PMap[code.Heartbeat] =  &processor.HeartbeatProcessor{Name: "Heartbeat"}
	processor.PMap[code.SendMessage] =  &processor.SendMessageProcessor{Name: "Send", Store: b.Store}
	processor.PMap[code.PullMessage] = &processor.PullMessageProcessor{Name: "Pull", Store: b.Store}
}

func (r *DefaultServer) InvokeSync(addr string, command *protocol.Command, timeoutMillis int64) (*protocol.Command, error) {
	channel := r.ChannelMap[addr]
	if channel == nil {
		log.Errorf("addr: %s", addr)
		return nil, errors.New("失败")
	}

	r.ResponseMap[command.Opaque] = &remoting.ResponseFuture{
		Opaque:          command.Opaque,
		Conn:            channel.Conn,
		BeginTimestamp:  time.Now().Unix(),
		TimeoutMillis:   timeoutMillis,
		DoneChan:        make(chan bool),
	}

	err := channel.WriteCommand(command)
	if err != nil {
		return nil, err
	}

	response, err := r.ResponseMap[command.Opaque].WaitResponse(timeoutMillis)
	return response, err
}

func (r *DefaultServer) InvokeAsync(addr string, command *protocol.Command, timeoutMillis int64, invokeCallback func(interface{}))  {
}

func (r *DefaultServer) AddChannel(conn net.Conn) *net2.Channel {

	channel := net2.Channel{
		Encoder: r.Encoder,
		Decoder: r.Decoder,
		Conn: conn,
	}

	r.ChannelMap[conn.RemoteAddr().String()] = &channel
	return &channel
}



func (r *DefaultServer) Start(b *BrokerController) {

	r.registerProcessor(b)

	listen, err := net.Listen("tcp4", "127.0.0.1:8089")
	if err != nil {
		panic(err)
	}

    go func() {
    	for {
			conn, err := listen.Accept()
			if err != nil {
				break
			}

			go r.handleConnection(conn)
		}
	}()

}

func (r *DefaultServer) handleConnection(conn net.Conn) {

	defer func() {
		delete(r.ChannelMap, conn.RemoteAddr().String())
	}()

	channel := r.AddChannel(conn)
	remoting.ReadMessage(*channel)

}



