package store

type AppendMessageStatus int32

const (
	AppendOk AppendMessageStatus = iota
	EndOfFile
	MessageSizeExceeded
	PropertiesSizeExceeded
	AppendUnknownError
)
