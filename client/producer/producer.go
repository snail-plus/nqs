package producer

import (
	"context"
	"nqs/client"
	"nqs/client/inner"
	"nqs/code"
	"nqs/common/message"
	"nqs/remoting/protocol"
	"time"
)

type Producer interface {
	SendSync(ctx context.Context, mq ...*message.Message) (*inner.SendResult, error)
}

type defaultProducer struct {
	group    string
	client   *client.RMQClient
	namesrvs client.Namesrvs
}

func NewDefaultProducer(group string) (*defaultProducer, error) {
	mqClient := client.GetOrNewRocketMQClient(client.Namesrvs{})
	return &defaultProducer{
		group:    group,
		client:   mqClient,
		namesrvs: client.Namesrvs{},
	}, nil
}

func (p *defaultProducer) Start() error {

	p.client.RegisterProducer(p.group, p)
	p.client.Start()
	return nil
}

func (p *defaultProducer) SendSync(ctx context.Context, msg *message.Message) (*inner.SendResult, error) {
	command := protocol.CreatesRequestCommand()
	command.Code = code.SendMessage
	command.CustomHeader = message.SendMessageRequestHeader{
		ProducerGroup: p.group,
		Topic:         msg.Topic,
		QueueId:       1,
		BornTimestamp: time.Now().Unix(),
	}

	// 从nameserv 获取地址 选择队列
	addr := p.namesrvs.FindBrokerAddrByName("aaa")
	_, err := p.client.InvokeSync(ctx, addr, command)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
