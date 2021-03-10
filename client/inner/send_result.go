package inner

import "nqs/common/message"

type SendStatus int

const (
	SendOK SendStatus = iota
	SendFlushDiskTimeout
	SendFlushSlaveTimeout
	SendSlaveNotAvailable
	SendUnknownError

	FlagCompressed = 0x1
	MsgIdLength    = 8 + 8

	propertySeparator  = '\002'
	nameValueSeparator = '\001'
)

type SendResult struct {
	Status        SendStatus
	MsgID         string
	MessageQueue  *message.MessageQueue
	QueueOffset   int64
	TransactionID string
	OffsetMsgID   string
	RegionID      string
	TraceOn       bool
}
