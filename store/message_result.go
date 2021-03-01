package store

import (
	"container/list"
	"nqs/common/message"
)

type GetMessageStatus int32

const (
	Found GetMessageStatus = iota
	NoMatchedMessage
	MessageWasRemoving
	OffsetFoundNull
	OffsetOverflowOne
	OffsetTooSmall
	OffsetOverflowBadly
	NoMessageInQueue
	NoMatchedLogicQueue
)

type GetMessageResult struct {
	Status           GetMessageStatus
	Messages         []*message.MessageExt
	NextBeginOffset  int64
	MinOffset        int64
	MaxOffset        int64
	MessageMapedList *list.List
	BufferTotalSize  int32
}

func (r *GetMessageResult) AddMessage(result *SelectMappedBufferResult) {
	if r.MessageMapedList == nil {
		r.MessageMapedList = list.New()
	}

	r.MessageMapedList.PushBack(result)
	r.BufferTotalSize += result.size
}
