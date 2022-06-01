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
	lock sync.Mutex
}

func CreateClient() *DefaultClient {
	remoting := Remoting{ResponseTable: sync.Map{}, ConnectionTable: sync.Map{}}
	remoting.Start()

	client := DefaultClient{
		Remoting: remoting,
	}
	return &client
}

func (r *DefaultClient) getOrCreateChannel(ctx context.Context, addr string) (*ch.Channel, error) {

	load, ok := r.ConnectionTable.Load(addr)
	if ok && !load.(*ch.Channel).Closed {
		return load.(*ch.Channel), nil
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)

	if err != nil {
		return nil, err
	}

	log.Infof("创建新连接, remoteAddress: %s", addr)

	newChannel := r.AddChannel(addr, conn)
	go r.ReadMessage(newChannel)
	go r.HandleWrite(newChannel)
	return newChannel, nil
}

func (r DefaultClient) InvokeOneWay(ctx context.Context, addr string, command *protocol.Command) error {
	channel, err := r.getOrCreateChannel(ctx, addr)
	if err != nil {
		return err
	}
	command.MarkOnewayRPC()
	err = channel.WriteCommand(command)
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

	err = channel.WriteCommand(command)

	if err != nil {
		log.Error("写入失败: %s", err.Error())
		r.RemoveChannel(channel)
		return nil, err
	}

	response, err := future.WaitResponse()
	return response, err
}

func (r *DefaultClient) InvokeAsync(ctx context.Context, addr string, command *protocol.Command, invokeCallback func(*protocol.Command, error)) error {
	channel, err := r.getOrCreateChannel(ctx, addr)
	if err != nil {
		return err
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

	err = channel.WriteCommand(command)
	if err != nil {
		invokeCallback(command, err)
	}

	return err
}

func (r *DefaultClient) HandleWrite(c *ch.Channel) {
	for !c.Closed {
		select {
		case response, isOpen := <-c.WriteChan:
			if !isOpen {
				break
			}

			err := c.WriteToConn(response)
			if err != nil {
				continue
			}
		}
	}
}
func (r *DefaultClient) Shutdown() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.ConnectionTable.Range(func(key, value interface{}) bool {
		value.(*ch.Channel).Destroy()
		return true
	})

	r.ResponseTable.Range(func(key, value interface{}) bool {
		r.ResponseTable.Delete(key)
		return true
	})

}
