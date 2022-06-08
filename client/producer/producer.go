package producer

import (
	"context"
	"nqs/client"
	"nqs/client/inner"
	"nqs/code"
	"nqs/common/message"
	"nqs/remoting/protocol"
	"nqs/util"
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
	command.Body = msg.Body
	command.CustomHeader = message.SendMessageRequestHeader{
		ProducerGroup: p.group,
		Topic:         msg.Topic,
		QueueId:       1,
		BornTimestamp: util.CurrentTimeMillis(),
	}

	// 从nameserv 获取地址 选择队列
	addr := p.namesrvs.FindBrokerAddrByName("aaa")
	response, err := p.client.InvokeSync(ctx, addr, command)
	if err != nil {
		return nil, err
	}

	result := &inner.SendResult{Status: inner.SendUnknownError}
	err = p.client.ProcessSendResponse("aaa", response, result, msg)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (p *defaultProducer) SendAsync(ctx context.Context, msg *message.Message, h func(context.Context, *inner.SendResult, error)) error {
	request := protocol.CreatesRequestCommand()
	request.Code = code.SendMessage
	request.Body = msg.Body
	now := util.CurrentTimeMillis()

	request.CustomHeader = message.SendMessageRequestHeader{
		ProducerGroup: p.group,
		Topic:         msg.Topic,
		QueueId:       1,
		BornTimestamp: now,
	}
	request.ExtFields["sendTime"] = util.CurrentTimeMillis()

	// 从nameserv 获取地址 选择队列
	addr := p.namesrvs.FindBrokerAddrByName("aaa")

	ctx, _ = context.WithTimeout(ctx, 1*time.Second)
	p.client.InvokeASync(ctx, addr, request, func(command *protocol.Command, err error) {
		if err != nil {
			h(ctx, nil, err)
		} else {
			result := &inner.SendResult{Status: inner.SendUnknownError}
			err := p.client.ProcessSendResponse("aaa", command, result, msg)
			h(ctx, result, err)
		}
	})

	return nil
}
