package processor

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"nqs/code"
	"nqs/common/message"
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
	"nqs/store"
	"nqs/util"
)

type PullMessageProcessor struct {
	Name  string
	Store *store.DefaultMessageStore
}

func (s *PullMessageProcessor) Reject() bool {
	return false
}

func (s *PullMessageProcessor) ProcessRequest(request *protocol.Command, channel *channel.Channel) {

	response := request.CreateResponseCommand()
	defer channel.WriteCommand(response)

	requestHeader := message.PullMessageRequestHeader{}
	err := util.MapToStruct(request.ExtFields, &requestHeader)
	if err != nil {
		response.Code = code.SystemError
		log.Error("MapToStruct error: " + err.Error())
		return
	}

	offset := requestHeader.QueueOffset
	manager := s.Store.ConsumerOffsetManager.Config.(*store.ConsumerOffsetManager)
	if offset < 0 {
		queryOffset := manager.QueryOffset(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueId)
		if queryOffset > 0 {
			offset = queryOffset
		}
		log.Infof("修复offset: %d", queryOffset)
	}

	getMessage := s.Store.GetMessage(requestHeader.ConsumerGroup, requestHeader.Topic, offset, requestHeader.QueueId, requestHeader.MaxMsgNums)
	getMessageStatus := int32(getMessage.Status)

	responseHeader := message.PullMessageResponseHeader{
		NextBeginOffset: getMessage.NextBeginOffset,
		MinOffset:       getMessage.MinOffset,
		MaxOffset:       getMessage.MaxOffset,
	}

	response.Code = getMessageStatus
	response.CustomHeader = responseHeader

	if getMessage.Status != store.Found {
		response.Code = int32(getMessage.Status)
		return
	}

	body := s.readGetMessageResult(getMessage)

	response.Body = body
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
