package main

import (
	log "github.com/sirupsen/logrus"
	"math"
	_ "net/http/pprof"
	"nqs/code"
	"nqs/common/message"
	_ "nqs/common/nlog"
	"nqs/remoting"
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
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
	go func() {
		msg := "test5502323"
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
			return
		}

		log.Infof("发送 response: %+v", response)
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
			time.Sleep(20 * time.Second)
		}
	}()

	go func() {
		for {
			response, err := defaultClient.PullMessage(addr, "testTopic", 0, 1, 2)
			if err != nil {
				log.Infof("PullMessage error : %s", err.Error())
				continue
			}

			log.Infof("pull 返回: %+v", response)
			time.Sleep(5 * time.Second)
		}
	}()

	time.Sleep(math.MaxInt64)

}
