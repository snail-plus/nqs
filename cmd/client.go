package main

import (
	log "github.com/sirupsen/logrus"
	"math"
	_ "net/http/pprof"
	"nqs/client/inner"
	"nqs/code"
	"nqs/common/message"
	_ "nqs/common/nlog"
	"nqs/remoting"
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
	"strconv"
	"time"
)

var defaultClient = remoting.DefaultClient{
	ChannelMap: map[string]*channel.Channel{},
	Encoder:    &protocol.JsonEncoder{},
	Decoder:    &protocol.JsonDecoder{},
}

const addr = "localhost:8089"

func main() {

	// write
	var index = 0
	go func() {
		for {
			msg := "msg-" + strconv.Itoa(index)
			command := protocol.CreatesRequestCommand()
			command.Code = code.SendMessage

			header := message.SendMessageRequestHeader{}
			header.BornTimestamp = time.Now().Unix()
			header.ProducerGroup = "test"
			header.Topic = "testTopic"
			header.QueueId = 1

			command.CustomHeader = header
			command.Body = []byte(msg)

			response, err2 := defaultClient.InvokeSync(addr, command, 2000)
			if err2 != nil {
				log.Errorf("err2: %s", err2.Error())
				continue
			}

			log.Infof("发送 response: %+v", response)
			time.Sleep(100 * time.Millisecond)
			index++
		}

	}()

	const MaxErrorCount = 20

	var errCount = 0
	go func() {
		for {
			heartbeat, err := defaultClient.SendHeartbeat(addr)
			errCount++
			if err != nil && errCount >= MaxErrorCount {
				break
			}

			log.Debugf("心跳返回: %+v", heartbeat)
			time.Sleep(50 * time.Second)
		}
	}()

	go func() {
		var offset int64 = 0
		for {
			time.Sleep(1 * time.Second)
			pullResult, err := defaultClient.PullMessage(addr, "testTopic", offset, 1, 200)
			if err != nil {
				log.Infof("PullMessage error : %s", err.Error())
				continue
			}

			if pullResult.NextBeginOffset > offset {
				offset = pullResult.NextBeginOffset
			}

			if pullResult.PullStatus != inner.Found {
				log.Infof("pull code: %d", int32(pullResult.PullStatus))
				continue
			}

			msgs := pullResult.MsgFoundList
			for item := msgs.Front(); item != nil; item = item.Next() {
				msg := item.Value.(*message.MessageExt)
				log.Infof("msg size: %d, pull 返回: %+v", msgs.Len(), msg)
			}

		}
	}()

	time.Sleep(math.MaxInt64)

}
