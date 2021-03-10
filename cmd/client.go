package main

import (
	"math"
	_ "net/http/pprof"
	"nqs/client/consumer"
	"nqs/common/message"
	_ "nqs/common/nlog"
	"time"
)

const addr = "localhost:8089"

func main() {

	//msgChan := make(chan *protocol.Command, 100000)
	// write

	/*msgBu := strings.Builder{}
	for i := 0; i < 5; i++ {
		msgBu.WriteString("a")
	}

	msg := msgBu.String()

	var responseCount = 0
	for index := 0; index < 1000000; index++ {

		command := protocol.CreatesRequestCommand()
		command.Code = code.SendMessage

		header := message.SendMessageRequestHeader{}
		header.BornTimestamp = time.Now().Unix()
		header.ProducerGroup = "test"
		header.Topic = "testTopic"
		header.QueueId = 1

		command.CustomHeader = header
		command.Body = []byte(msg + strconv.Itoa(index))

		defaultClient.InvokeAsync(addr, command, 2000, func(response *protocol.Command, err error) {

			if err != nil {
				log.Errorf("error: %s", err.Error())
				return
			}

			if responseCount%10000 == 0 {
				log.Infof("响应次数: %d", responseCount)
			}

			responseCount++

		})

	}*/

	/*go func() {
		index := 0
		for {
			select {
			case command := <-msgChan:
				index ++
				_, err := defaultClient.InvokeSync(addr, command, 2000)
				if err != nil {
					log.Errorf("error: %s", err.Error())
					continue
				}

				if index % 10000 == 0{
					log.Infof("响应次数: %d", index)
				}

				/*defaultClient.InvokeAsync(addr, command, 2000, func(response *protocol.Command, err error) {
					index ++
					if err != nil {
						log.Errorf("error: %s", err.Error())
					} else if index % 10000 == 0{
						log.Infof("响应: %v, 次数: %d", response, index)
					}
				})
			default:
				continue
			}
		}
	}()*/

	pushConsumer, err := consumer.NewPushConsumer("test", "testTopic")
	if err != nil {
		panic(err)
	}

	pushConsumer.ConsumeMsg = func(ext []*message.MessageExt) {
		/*for _, item := range ext {
			log.Infof("收到消息: %+v", item)
		}*/
	}

	pushConsumer.Start()
	time.Sleep(math.MaxInt64)

}
