package store

type PutMessageStatus int32

const (
	PutOk PutMessageStatus = iota
	FlushDiskTimeout
	UnknownError
	CreateMappedFileFailed
)

type AppendMessageResult struct {
	Status         AppendMessageStatus
	WroteOffset    int64
	WroteBytes     int32
	MsgId          string
	StoreTimestamp int64
	LogicsOffset   int64
}

type PutMessageResult struct {
	PutMessageStatus    PutMessageStatus
	AppendMessageResult AppendMessageResult
}
