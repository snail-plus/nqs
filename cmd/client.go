package main

import (
	"context"
	log "github.com/sirupsen/logrus"
	"math"
	_ "net/http/pprof"
	"nqs/client/consumer"
	"nqs/client/inner"
	"nqs/client/producer"
	"nqs/common/message"
	_ "nqs/common/nlog"
	"nqs/util"
	"time"
)

const addr = "localhost:8089"

func main() {

	time.AfterFunc(3*time.Second, func() {
		defaultProducer, err2 := producer.NewDefaultProducer("xx")

		if err2 != nil {
			return
		}

		for i := 0; i < 10; i++ {
			randomString := util.RandomString(i + 3)
			msg := &message.Message{
				Topic: "testTopic",
				Body:  []byte(randomString),
				Flag:  0,
			}

			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
			defaultProducer.SendAsync(ctx, msg, func(ctx context.Context, result *inner.SendResult, err error) {
				if err != nil {
					return
				}

				log.Infof("send result: %+v", result)
			})
			// time.Sleep(1 * time.Second)
		}
	})

	pushConsumer, err := consumer.NewPushConsumer("test", "testTopic")
	if err != nil {
		panic(err)
	}

	pushConsumer.ConsumeMsg = func(ext []*message.MessageExt) {
		for _, item := range ext {
			delay := time.Now().UnixNano()/1e6 - item.BornTimestamp
			log.Infof("延迟: %d ms, 收到消息: %+v", delay, item)
		}
	}

	pushConsumer.Start()
	time.Sleep(math.MaxInt64)

}
