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

	log.Infof("requestHeader: %+v", requestHeader)

	getMessage := s.Store.GetMessage(requestHeader.ConsumerGroup, requestHeader.Topic, requestHeader.QueueOffset, requestHeader.QueueId, requestHeader.MaxMsgNums)
	getMessageStatus := int32(getMessage.Status)

	if getMessage.Status != store.Found {
		response.Code = int32(getMessage.Status)
		return
	}

	body := s.readGetMessageResult(getMessage)

	log.Infof("queueOffset: %d, founded msg size: %d, NextBeginOffset: %d, getMessageStatus: %d, response len: %d",
		requestHeader.QueueOffset, getMessage.GetMsgSize(), getMessage.NextBeginOffset, getMessageStatus, len(body))

	responseHeader := message.PullMessageResponseHeader{
		NextBeginOffset: getMessage.NextBeginOffset,
		MinOffset:       getMessage.MinOffset,
		MaxOffset:       getMessage.MaxOffset,
	}

	response.Code = getMessageStatus
	response.CustomHeader = responseHeader
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
		log.Infof("readGetMessageResult, total Size: %d", util.BytesToInt32(body[0:4]))
	}

	return buffer.Bytes()
}
