package main

import (
	"context"
	log "github.com/sirupsen/logrus"
	"math"
	_ "net/http/pprof"
	"nqs/client/consumer"
	"nqs/client/producer"
	"nqs/common/message"
	_ "nqs/common/nlog"
	"time"
)

const addr = "localhost:8089"

func main() {

	defaultProducer, err2 := producer.NewDefaultProducer("xx")

	if err2 != nil {
		return
	}

	msg := &message.Message{
		Topic: "testTopic",
		Body:  []byte("abcd"),
		Flag:  0,
	}

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	result, err2 := defaultProducer.SendSync(ctx, msg)
	if err2 != nil {
		return
	}

	log.Infof("send result: %+v", result)

	pushConsumer, err := consumer.NewPushConsumer("test", "testTopic")
	if err != nil {
		panic(err)
	}

	pushConsumer.ConsumeMsg = func(ext []*message.MessageExt) {
		for _, item := range ext {
			log.Infof("收到消息: %+v", item)
		}
	}

	pushConsumer.Start()
	time.Sleep(math.MaxInt64)

}
