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
	"strconv"
	"strings"
	"time"
)

var defaultClient = remoting.DefaultClient{
	ChannelMap: map[string]*channel.Channel{},
	Encoder:    &protocol.JsonEncoder{},
	Decoder:    &protocol.JsonDecoder{},
}

const addr = "localhost:8089"

func main() {

	msgChan := make(chan *protocol.Command, 100000)
	// write
	var index = 0

	msgBu := strings.Builder{}
	for i := 0; i < 1024-400; i++ {
		msgBu.WriteString("a")
	}

	msg := msgBu.String()

	go func() {
		for {

			if index > 200000 {
				break
			}

			command := protocol.CreatesRequestCommand()
			command.Code = code.SendMessage

			header := message.SendMessageRequestHeader{}
			header.BornTimestamp = time.Now().Unix()
			header.ProducerGroup = "test"
			header.Topic = "testTopic"
			header.QueueId = 1

			command.CustomHeader = header
			command.Body = []byte(msg + strconv.Itoa(index))

			msgChan <- command

			if index%10000 == 0 {
				log.Infof("发送 条数: %d", index)
			}

			// log.Infof("发送 response: %+v", response)
			index++
		}

	}()

	go func() {
		for {
			select {
			case command := <-msgChan:
				defaultClient.InvokeAsync(addr, command, 2000, func(response *protocol.Command, err error) {
					if err != nil {
						log.Error("error: %s", err.Error())
					} else {
						log.Infof("响应: %v", response)
					}
				})
			default:
				continue
			}
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

	/*pushConsumer, err := consumer.NewPushConsumer("test", "testTopic")
	if err != nil {
		panic(err)
	}

	pushConsumer.ConsumeMsg = func(ext []*message.MessageExt) {
		for _, item := range ext {
			log.Infof("收到消息: %+v", item)
		}
	}

	pushConsumer.Start()
	*/
	time.Sleep(math.MaxInt64)

}
