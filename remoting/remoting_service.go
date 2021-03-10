package remoting

import (
	"context"
	"errors"
	"fmt"
	"github.com/henrylee2cn/goutil/calendar/cron"
	log "github.com/sirupsen/logrus"
	"net"
	"nqs/code"
	"nqs/processor"
	ch "nqs/remoting/channel"
	"nqs/remoting/protocol"
	"nqs/util"
	"sync"
)

type Remote interface {
	InvokeSync(ctx context.Context, addr string, command *protocol.Command) (*protocol.Command, error)
	InvokeAsync(ctx context.Context, addr string, command *protocol.Command, invokeCallback func(*protocol.Command, error))
	InvokeOneWay(ctx context.Context, addr string, command *protocol.Command) error
	AddChannel(addr string, conn net.Conn) *ch.Channel
	Shutdown()
}

type Remoting struct {
	Remote
	once            sync.Once
	ResponseTable   sync.Map
	ConnectionTable sync.Map
	cron            *cron.Cron
}

func (r *Remoting) AddChannel(addr string, conn net.Conn) *ch.Channel {
	channel := &ch.Channel{
		Conn:      conn,
		WriteChan: make(chan *protocol.Command, 1000),
	}
	r.ConnectionTable.Store(addr, channel)
	return channel
}

func (r *Remoting) CloseChannel(channel *ch.Channel) {
	r.ConnectionTable.Range(func(key, value interface{}) bool {
		if value == channel {
			r.ConnectionTable.Delete(channel)
		}
		return true
	})
	channel.Destroy()
}

func (r *Remoting) ScanConnectionTable() {
	var futureList = make([]*ResponseFuture, 0)
	r.ResponseTable.Range(func(key, value interface{}) bool {
		future := value.(*ResponseFuture)
		if future.IsTimeout() {
			r.ResponseTable.Delete(key)
			futureList = append(futureList, future)
			log.Warnf("请求过期, 过期 key: %v", key)
		}
		return true
	})

	for _, item := range futureList {
		if item.InvokeCallback != nil {
			item.InvokeCallback(nil, errors.New(fmt.Sprintf("请求超时,op: %d", item.Opaque)))
		}
	}
}

func (r *Remoting) Start() {
	r.once.Do(func() {
		r.cron = cron.New()
		r.cron.AddFunc("./10 * * * * ?", func() {
			r.ScanConnectionTable()
		})
	})
}

func (r *Remoting) processMessageReceived(command *protocol.Command, channel *ch.Channel) {

	// 处理响应
	if command.IsResponseType() {
		r.processResponseCommand(command, channel)
		return
	}

	// 处理请求
	pair, ok := processor.PMap[command.Code]
	if !ok {
		var errorCommand = &protocol.Command{Code: code.SystemError, Opaque: command.Opaque, Flag: 1}
		channel.WriteCommand(errorCommand)
		log.Errorf("Code: %d 没有找到对应的处理器", command.Code)
		return
	}

	pair.Pool.Submit(func() {
		pair.Processor.ProcessRequest(command, channel)
	})

}

func (r *Remoting) processResponseCommand(command *protocol.Command, channel *ch.Channel) {
	value, ok := r.ResponseTable.Load(command.Opaque)

	if !ok {
		log.Errorf("Opaque %d 找不到对应的请求,address: %s", command.Opaque, channel.RemoteAddr())
		return
	}

	r.ResponseTable.Delete(command.Opaque)
	future := value.(*ResponseFuture)
	callback := future.InvokeCallback
	if callback != nil {
		callback(command, nil)
	} else {
		future.PutResponse(*command)
	}

}

func (r *Remoting) ReadMessage(channel *ch.Channel) {
	conn := channel.Conn

	for !channel.Closed {
		head, err := ch.ReadFully(ch.HeadLength, conn)
		if err != nil {
			log.Errorf("读取头部错误, %+v", err)
			break
		}

		headLength := util.BytesToInt32(head)

		if headLength <= 0 {
			log.Infof("数据异常: %d", headLength)
			break
		}

		remainData, err := ch.ReadFully(headLength, conn)
		if err != nil {
			log.Errorf("读取头部错误, %+v", err)
			break
		}

		log.Debugf("remainData length: %d", len(remainData))
		command, err := protocol.Decode(remainData)
		if err != nil {
			log.Error("decode 失败, ", err.Error())
			continue
		}

		r.processMessageReceived(command, channel)

	}

	log.Warnf("读完成,conn: %s", conn.RemoteAddr().String())
}
