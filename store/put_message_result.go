package store


type PutMessageStatus int32
const (
	PutOk PutMessageStatus = 1
	FlushDiskTimeout
	UnknownError
	CreateMappedFileFailed
)


type AppendMessageResult struct {
	WroteOffset int64
	MsgId string
	StoreTimestamp int64
	LogicsOffset int64
}

type PutMessageResult struct {
	PutMessageStatus PutMessageStatus
	AppendMessageResult AppendMessageResult
}
