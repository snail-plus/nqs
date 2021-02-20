package remoting

import (
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
	TimeoutMillis   int64
	DoneChan        chan bool
	ResponseCommand protocol.Command
}


func (r *ResponseFuture) WaitResponse(timeoutMillis int64) (*protocol.Command, error) {
	select {
	case  <- r.DoneChan:
		log.Debugf("WaitResponse %+v, address: %s, localAddress: %s", r.ResponseCommand,
			r.Conn.RemoteAddr().String(), r.Conn.LocalAddr().String())
		return &r.ResponseCommand, nil
	case <- time.After(time.Millisecond * time.Duration(timeoutMillis)):
		return nil, errors.New("超时")
	}
}

func (r *ResponseFuture) PutResponse(command protocol.Command)  {
	r.ResponseCommand = command
	log.Debugf("PutResponse %+v, remote address: %s, localAddress: %s", r.ResponseCommand,
		r.Conn.RemoteAddr().String(), r.Conn.LocalAddr().String())
	r.DoneChan <- true
}

func (r *ResponseFuture) IsTimeout() bool {
	return time.Now().Unix() - r.BeginTimestamp > r.TimeoutMillis
}