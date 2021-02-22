package store

type AppendMessageStatus int32

const (
	AppendOk AppendMessageStatus = 1
	EndOfFile
	MessageSizeExceeded
	PropertiesSizeExceeded
	AppendUnknownError
)
