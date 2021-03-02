package remoting

import (
	"bytes"
	"container/list"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"net"
	"nqs/client/consumer"
	"nqs/code"
	"nqs/common/message"
	"nqs/common/protocol/heartbeat"
	net2 "nqs/remoting/channel"
	"nqs/remoting/protocol"
	"nqs/store"
	"nqs/util"
	"os"
	"strconv"
	"sync"
	"time"
)

type DefaultClient struct {
	lock       sync.Mutex
	ChannelMap map[string]*net2.Channel
	Encoder    protocol.Encoder
	Decoder    protocol.Decoder
}

func (r *DefaultClient) getOrCreateChannel(addr string) (*net2.Channel, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if v, ok := r.ChannelMap[addr]; ok {
		log.Debugf("获取到老连接，address: %s", v.Conn.RemoteAddr().String())
		return v, nil
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	log.Infof("创建新连接, remoteAddress: %s", addr)

	newChannel := r.AddChannel(conn, addr)
	go ReadMessage(*newChannel)
	return newChannel, nil
}

func (r *DefaultClient) InvokeSync(addr string, command *protocol.Command, timeoutMillis int64) (*protocol.Command, error) {
	channel, err := r.getOrCreateChannel(addr)
	if err != nil {
		return nil, err
	}

	ResponseMap[command.Opaque] = &ResponseFuture{
		Opaque:         command.Opaque,
		Conn:           channel.Conn,
		BeginTimestamp: time.Now().Unix(),
		TimeoutMillis:  timeoutMillis,
		DoneChan:       make(chan bool),
	}

	err = channel.WriteCommand(command)

	if err != nil {
		r.closeChannel(addr, channel)
		return nil, err
	}

	response, err := ResponseMap[command.Opaque].WaitResponse(timeoutMillis)
	return response, err
}

func (r *DefaultClient) InvokeAsync(addr string, command *protocol.Command, timeoutMillis int64, invokeCallback func(interface{})) {
}

func (r *DefaultClient) AddChannel(conn net.Conn, addr string) *net2.Channel {
	channel := net2.Channel{
		Encoder: r.Encoder,
		Decoder: r.Decoder,
		Conn:    conn,
	}

	r.ChannelMap[addr] = &channel
	log.Infof("ChannelMap: %+v", r.ChannelMap)
	return &channel
}

func (r *DefaultClient) SendHeartbeat(addr string) (*protocol.Command, error) {

	heartbeatData := heartbeat.Heartbeat{ClientId: util.GetLocalAddress() + "@" + strconv.Itoa(os.Getpid())}
	body, err := json.Marshal(heartbeatData)
	if err != nil {
		return nil, err
	}

	command := protocol.CreatesRequestCommand()
	command.Code = code.Heartbeat
	command.Body = body

	response, err := r.InvokeSync(addr, command, 3000)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (r *DefaultClient) PullMessage(addr, topic string, offset int64, queueId, maxMsgCount int32) (*consumer.PullResult, error) {
	header := message.PullMessageRequestHeader{}
	header.Topic = topic
	header.QueueId = queueId
	header.MaxMsgNums = maxMsgCount
	header.QueueOffset = offset

	command := protocol.CreatesRequestCommand()
	command.Code = code.PullMessage
	command.CustomHeader = header

	response, err := r.InvokeSync(addr, command, 3000)
	if err != nil {
		return nil, err
	}

	// body := response.Body
	// decode body 哈哈
	responseHeader := message.PullMessageResponseHeader{}
	err = util.MapToStruct(response.ExtFields, &responseHeader)
	if err != nil {
		return nil, err
	}

	pullResult := &consumer.PullResult{MsgFoundList: list.New(),
		NextBeginOffset: responseHeader.NextBeginOffset, MinOffset: responseHeader.MinOffset,
		MaxOffset: responseHeader.MaxOffset,
	}

	if response.Code != int32(store.Found) {
		return pullResult, nil
	}

	// decode msg body
	bodyByteBuffer := bytes.NewBuffer(response.Body)
	for bodyByteBuffer.Len() > 0 {
		messageExt := message.DecodeMsg(bodyByteBuffer, false)
		pullResult.MsgFoundList.PushBack(messageExt)
	}

	return pullResult, nil
}

func (r *DefaultClient) closeChannel(addr string, channel *net2.Channel) {
	r.lock.Lock()
	defer r.lock.Unlock()

	channel.Closed = true
	channel.Conn.Close()
	delete(r.ChannelMap, addr)
	log.Infof("close Channel: %+v", channel)
}
