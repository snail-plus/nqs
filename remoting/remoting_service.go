package remoting

import (
	"errors"
	"fmt"
	"github.com/henrylee2cn/goutil/calendar/cron"
	log "github.com/sirupsen/logrus"
	"net"
	"nqs/code"
	"nqs/processor"
	net2 "nqs/remoting/channel"
	"nqs/remoting/protocol"
	"nqs/util"
	"sync"
	"time"
)

type Remote interface {
	InvokeSync(addr string, command *protocol.Command, timeoutMillis int64) (*protocol.Command, error)
	InvokeAsync(addr string, command *protocol.Command, timeoutMillis int64, invokeCallback func(interface{}))

	AddChannel(conn net.Conn) *net2.Channel
}

var ResponseMap = sync.Map{} /*map[int32]*ResponseFuture{}*/

func init() {
	c := cron.New()
	c.AddFunc("*/10 * * * * ?", func() {
		var futureList = make([]*ResponseFuture, 0)
		ResponseMap.Range(func(key, value interface{}) bool {
			future := value.(*ResponseFuture)
			if future.BeginTimestamp+future.TimeoutMillis+1000 < time.Now().Unix() {
				ResponseMap.Delete(key)
				futureList = append(futureList, future)
				log.Warnf("过期 key: %v", key)
			}
			return true
		})

		for _, item := range futureList {
			if item.InvokeCallback != nil {
				item.InvokeCallback(nil, errors.New(fmt.Sprintf("请求超时,op: %d", item.Opaque)))
			}
		}

	})
}

func processMessageReceived(command *protocol.Command, channel *net2.Channel) {

	defer func() {
		err := recover()
		if err != nil {
			log.Error("handleConnection error: ", err)
		}
	}()

	// 处理响应
	if command.Flag != 0 {
		processResponseCommand(command, channel)
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

func processResponseCommand(command *protocol.Command, channel *net2.Channel) {
	value, ok := ResponseMap.Load(command.Opaque)

	if !ok {
		log.Errorf("Opaque %d 找不到对应的请求,address: %s", command.Opaque, channel.RemoteAddr())
		return
	}
	ResponseMap.Delete(command.Opaque)
	future := value.(*ResponseFuture)
	callback := future.InvokeCallback
	if callback != nil {
		callback(command, nil)
	} else {
		future.PutResponse(*command)
	}

}

func ReadMessage(channel *net2.Channel) {
	conn := channel.Conn
	decoder := channel.Decoder

	for !channel.Closed {
		head, err := net2.ReadFully(net2.HeadLength, conn)
		if err != nil {
			log.Errorf("读取头部错误, %+v", err)
			break
		}

		headLength := util.BytesToInt32(head)

		if headLength <= 0 {
			log.Infof("数据异常: %d", headLength)
			break
		}

		remainData, err := net2.ReadFully(headLength, conn)
		if err != nil {
			log.Errorf("读取头部错误, %+v", err)
			break
		}

		log.Debugf("remainData length: %d", len(remainData))
		command, err := decoder.Decode(remainData)
		if err != nil {
			log.Error("decode 失败, ", err.Error())
			continue
		}

		processMessageReceived(command, channel)

	}

	log.Warnf("读完成,conn: %s", conn.RemoteAddr().String())
}
