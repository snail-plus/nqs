package remoting

import (
	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"net"
	"nqs/processor"
	net2 "nqs/remoting/channel"
	"nqs/remoting/protocol"
	"nqs/util"
	"sync"
)

type Remote interface {
	InvokeSync(addr string, command *protocol.Command, timeoutMillis int64) (*protocol.Command, error)
	InvokeAsync(addr string, command *protocol.Command, timeoutMillis int64, invokeCallback func(interface{}))

	AddChannel(conn net.Conn) *net2.Channel
}

var ResponseMap = sync.Map{} /*map[int32]*ResponseFuture{}*/

func processMessageReceived(command *protocol.Command, channel net2.Channel) {

	// 处理响应
	if command.Flag != 0 {
		processResponseCommand(command, channel)
		return
	}

	// 处理请求
	processor := processor.PMap[command.Code]
	if processor == nil {
		log.Errorf("Code: %d 没有找到对应的处理器", command.Code)
		return
	}

	processor.ProcessRequest(command, &channel)

}

func processResponseCommand(command *protocol.Command, channel net2.Channel) {
	value, ok := ResponseMap.Load(command.Opaque)

	if !ok {
		log.Errorf("Opaque %d 找不到对应的请求,address: %s", command.Opaque, channel.RemoteAddr())
		return
	}
	future := value.(*ResponseFuture)
	future.PutResponse(*command)
	ResponseMap.Delete(command.Opaque)
}

func ReadMessage(channel net2.Channel) {
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

		ants.Submit(func() {
			// 处理请求

			defer func() {
				err := recover()
				if err != nil {
					log.Error("handleConnection error: ", err)
				}

			}()
			// remainData 数据 头部长度 + 头部数据 + BODY
			log.Debugf("remainData length: %d", len(remainData))
			command, err := decoder.Decode(remainData)
			if err != nil {
				log.Error("decode 失败, ", err.Error())
				return
			}

			processMessageReceived(command, channel)
		})

	}

}
