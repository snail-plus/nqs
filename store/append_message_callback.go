package store

import "github.com/edsrzf/mmap-go"

type AppendMessageCallback interface {
	DoAppend(fileMap mmap.MMap, currentOffset int32, fileFromOffset int64, maxBlank int32, inner *MessageExtBrokerInner) *AppendMessageResult
}
