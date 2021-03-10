package remoting

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"net"
	"nqs/remoting/protocol"
	"time"
)

type ResponseFuture struct {
	Opaque          int32
	Conn            net.Conn
	BeginTimestamp  int64
	Ctx             context.Context
	DoneChan        chan bool
	ResponseCommand protocol.Command
	InvokeCallback  func(*protocol.Command, error)
}

func (r *ResponseFuture) WaitResponse() (*protocol.Command, error) {
	select {
	case <-r.DoneChan:
		log.Debugf("WaitResponse %+v, address: %s, localAddress: %s", r.ResponseCommand,
			r.Conn.RemoteAddr().String(), r.Conn.LocalAddr().String())
		cost := time.Now().Unix() - r.BeginTimestamp
		if cost > 1 {
			log.Infof("响应过慢, cost: %d", cost)
		}
		return &r.ResponseCommand, nil
	case <-r.Ctx.Done():
		return nil, errors.New("超时")
	}
}

func (r *ResponseFuture) PutResponse(command protocol.Command) {
	r.ResponseCommand = command
	close(r.DoneChan)
}

func (r *ResponseFuture) IsTimeout() bool {
	select {
	case <-r.Ctx.Done():
		return true
	default:
		return false
	}
}
