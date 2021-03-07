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
	"nqs/store"
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

	msgChan := make(chan *protocol.Command, 100)
	// write
	var index = 0
	builder := strings.Builder{}
	for i := 0; i < 2040; i++ {
		builder.WriteString("a")
	}

	go func() {
		for {
			msg := builder.String() + "-" + strconv.Itoa(index)
			command := protocol.CreatesRequestCommand()
			command.Code = code.SendMessage

			header := message.SendMessageRequestHeader{}
			header.BornTimestamp = time.Now().Unix()
			header.ProducerGroup = "test"
			header.Topic = "testTopic"
			header.QueueId = 1

			command.CustomHeader = header
			command.Body = []byte(msg)

			msgChan <- command

			if index > 1000000 {
				break
			}

			if index%10000 == 0 {
				log.Infof("发送 条数: %d", index)
			}

			// log.Infof("发送 response: %+v", response)
			index++
		}

	}()

	for i := 0; i < 2; i++ {
		go func() {
			for {
				select {
				case command := <-msgChan:
					startTime := time.Now()

					_, err2 := defaultClient.InvokeSync(addr, command, 2000)
					store.IncResponseCost(time.Since(startTime).Nanoseconds())
					if err2 != nil {
						log.Errorf("err2: %s", err2.Error())
						continue
					}
				default:
					continue
				}
			}
		}()
	}

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

	pushConsumer.Start()*/

	time.Sleep(math.MaxInt64)

}
