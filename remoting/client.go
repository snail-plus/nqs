package remoting

import (
	"context"
	log "github.com/sirupsen/logrus"
	"net"
	ch "nqs/remoting/channel"
	"nqs/remoting/protocol"
	"sync"
	"time"
)

type DefaultClient struct {
	Remoting
	lock       sync.Mutex
	ChannelMap map[string]*ch.Channel
}

func CreateClient() *DefaultClient {
	remoting := Remoting{ResponseTable: sync.Map{}, ConnectionTable: sync.Map{}}
	remoting.Start()
	return &DefaultClient{
		Remoting:   remoting,
		ChannelMap: map[string]*ch.Channel{},
	}
}

func (r *DefaultClient) getOrCreateChannel(ctx context.Context, addr string) (*ch.Channel, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if v, ok := r.ChannelMap[addr]; ok {
		log.Debugf("获取到老连接，address: %s", v.Conn.RemoteAddr().String())
		return v, nil
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)

	if err != nil {
		return nil, err
	}

	log.Infof("创建新连接, remoteAddress: %s", addr)

	newChannel := r.AddChannel(addr, conn)
	go r.ReadMessage(newChannel)
	return newChannel, nil
}

func (r DefaultClient) InvokeOneWay(ctx context.Context, addr string, command *protocol.Command) error {
	channel, err := r.getOrCreateChannel(ctx, addr)
	if err != nil {
		return err
	}
	command.MarkOnewayRPC()
	err = channel.WriteToConn(command)
	return err
}

func (r *DefaultClient) InvokeSync(ctx context.Context, addr string, command *protocol.Command) (*protocol.Command, error) {
	now := time.Now()
	channel, err := r.getOrCreateChannel(ctx, addr)
	since := time.Since(now)
	if since.Seconds() > 0.5 {
		log.Infof("获取连接耗时过长")
	}

	if err != nil {
		return nil, err
	}

	future := &ResponseFuture{
		Opaque:         command.Opaque,
		Conn:           channel.Conn,
		BeginTimestamp: time.Now().Unix(),
		Ctx:            ctx,
		DoneChan:       make(chan bool),
	}

	r.ResponseTable.Store(command.Opaque, future)

	err = channel.WriteToConn(command)

	if err != nil {
		r.closeChannel(channel)
		return nil, err
	}

	response, err := future.WaitResponse()
	return response, err
}

func (r *DefaultClient) InvokeAsync(ctx context.Context, addr string, command *protocol.Command, invokeCallback func(*protocol.Command, error)) {
	channel, err := r.getOrCreateChannel(ctx, addr)
	if err != nil {
		invokeCallback(nil, err)
		return
	}

	future := &ResponseFuture{
		Opaque:         command.Opaque,
		Conn:           channel.Conn,
		BeginTimestamp: time.Now().Unix(),
		Ctx:            ctx,
		DoneChan:       make(chan bool),
		InvokeCallback: invokeCallback,
	}

	r.ResponseTable.Store(command.Opaque, future)

	err = channel.WriteToConn(command)
	if err != nil {
		invokeCallback(nil, err)
	}

}

func (r *DefaultClient) closeChannel(channel *ch.Channel) {
	r.lock.Lock()
	defer r.lock.Unlock()

	for addr, item := range r.ChannelMap {
		if item == channel {
			delete(r.ChannelMap, addr)
			channel.Destroy()
			break
		}
	}

	log.Infof("close Channel: %+v", channel)
}

func (r *DefaultClient) Shutdown() {
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, item := range r.ChannelMap {
		item.Destroy()
	}

	r.ChannelMap = nil

}
