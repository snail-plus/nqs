package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math"
	_ "net/http/pprof"
	"nqs/client/consumer"
	"nqs/client/inner"
	"nqs/client/producer"
	"nqs/common/message"
	_ "nqs/common/nlog"
	"nqs/util"
	"sync/atomic"
	"time"
)

const addr = "localhost:8089"

func main() {

	time.AfterFunc(3*time.Second, func() {
		defaultProducer, err2 := producer.NewDefaultProducer("xx")

		if err2 != nil {
			return
		}

		ctx := context.Background()
		startTime := util.CurrentTimeMillis()
		for i := 0; i < 150000; i++ {
			// randomString := util.RandomString(100)
			randomString := "abcdeftgy" + fmt.Sprintf("%d", i)
			msg := &message.Message{
				Topic: "testTopic",
				Body:  []byte(randomString),
				Flag:  0,
			}

			defaultProducer.SendAsync(ctx, msg, func(ctx context.Context, result *inner.SendResult, err error) {
				if err != nil {
					return
				}

				// log.Infof("send result: %+v", result)
			})

			// time.Sleep(1 * time.Second)
		}

		log.Infof("发送耗时: %d ms", util.CurrentTimeMillis()-startTime)
	})

	pushConsumer, err := consumer.NewPushConsumer("test", "testTopic")
	if err != nil {
		panic(err)
	}

	var counter int32 = 0
	pushConsumer.ConsumeMsg = func(ext []*message.MessageExt) {
		for _, item := range ext {
			value := atomic.AddInt32(&counter, 1)
			delay1 := time.Now().UnixNano()/1e6 - item.BornTimestamp
			delay2 := item.StoreTimestamp - item.BornTimestamp
			//delay := item.StoreTimestamp - item.BornTimestamp
			if (delay1 > 1 || delay2 > 1) && value%100 == 0 {
				log.Infof("延迟1: %d ms, 延迟2: %d ms, 收到消息: %+v", delay1, delay2, item)
			}
		}
	}

	pushConsumer.Start()
	time.Sleep(math.MaxInt64)

}
