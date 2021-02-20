package store

import "nqs/common/message"


type GetMessageStatus int32
const (
	Found GetMessageStatus = 1
	NoMatchedMessage
	MessageWasRemoving
	OffsetFoundNull
	OffsetOverflowOne
	OffsetTooSmall
	NoMessageInQueue
)

type GetMessageResult struct {
	Status GetMessageStatus
	Messages []*message.MessageExt
}

