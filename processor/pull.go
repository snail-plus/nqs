package processor

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"nqs/broker/longpolling"
	"nqs/code"
	"nqs/common/message"
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
	"nqs/store"
	"nqs/util"
	"time"
)

type PullMessageProcessor struct {
	Name                   string
	Store                  *store.DefaultMessageStore
	PullRequestHoldService longpolling.LongPolling
}

func (s *PullMessageProcessor) Reject() bool {
	return false
}

func (s *PullMessageProcessor) ProcessRequest(request *protocol.Command, channel *channel.Channel) {

	response := request.CreateResponseCommand()

	requestHeader := message.PullMessageRequestHeader{}
	err := util.MapToStruct(request.ExtFields, &requestHeader)
	if err != nil {
		response.Code = code.SystemError
		log.Error("MapToStruct error: " + err.Error())
		channel.WriteCommand(response)
		return
	}

	log.Debugf("收到查询msg 请求, topic: %s, offset: %d", requestHeader.Topic, requestHeader.QueueOffset)

	offset := requestHeader.QueueOffset

	if offset < 0 {
		manager := s.Store.ConsumerOffsetManager.Config.(*store.ConsumerOffsetManager)
		queryOffset := manager.QueryOffset(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId)
		if queryOffset > 0 {
			requestHeader.QueueOffset = queryOffset
			offset = queryOffset
		}
		log.Infof("修复offset: %d", queryOffset)
	}

	getMessageResult := s.Store.GetMessage(requestHeader.ConsumerGroup, requestHeader.Topic, offset, requestHeader.QueueId, requestHeader.MaxMsgNums)
	getMessageStatus := int32(getMessageResult.Status)

	responseHeader := message.PullMessageResponseHeader{
		NextBeginOffset: getMessageResult.NextBeginOffset,
		MinOffset:       getMessageResult.MinOffset,
		MaxOffset:       getMessageResult.MaxOffset,
	}

	response.Code = getMessageStatus
	response.CustomHeader = responseHeader

	switch getMessageResult.Status {
	case store.Found:
		body := s.readGetMessageResult(getMessageResult)
		response.Body = body

	case store.NoMessageInQueue, store.OffsetOverflowOne:
		if requestHeader.SuspendTimeoutMillis > 0 {
			pullRequest := &longpolling.PullRequest{
				ClientChannel:      channel,
				PullFromThisOffset: requestHeader.QueueOffset,
				RequestCommand:     request,
				TimeoutMillis:      requestHeader.SuspendTimeoutMillis,
				SuspendTimestamp:   time.Now().UnixNano() / 1e6,
			}
			s.PullRequestHoldService.SuspendPullRequest(requestHeader.Topic, requestHeader.QueueId, pullRequest)
			response = nil
		} else {
			response.Code = int32(store.NoMessageInQueue)
		}
	default:
		response.Code = int32(store.NoMessageInQueue)
	}

	channel.WriteCommand(response)
}

func (s *PullMessageProcessor) readGetMessageResult(getResult *store.GetMessageResult) []byte {
	byteBuff := make([]byte, getResult.BufferTotalSize)
	buffer := bytes.NewBuffer(byteBuff[:0])

	mapedList := getResult.MessageMapedList
	for item := mapedList.Front(); item != nil; item = item.Next() {
		msg := item.Value.(*store.SelectMappedBufferResult)
		body := msg.ByteBuffer.Bytes()
		buffer.Write(body)
	}

	return buffer.Bytes()
}
