package store

type AppendMessageCallback interface {
	DoAppend(fileFromOffset int64, maxBlank int32, inner *MessageExtBrokerInner) *AppendMessageResult
}

type DefaultAppendMessageCallback struct {

}

func (r DefaultAppendMessageCallback) DoAppend(fileFromOffset int64, maxBlank int32, ext *MessageExtBrokerInner) *AppendMessageResult {
	return nil
}