package store

type AppendMessageCallback interface {
	DoAppend(fileFromOffset int64, maxBlank int32, inner *MessageExtBrokerInner) *AppendMessageResult
}
