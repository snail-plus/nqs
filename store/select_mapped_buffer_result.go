package store

import (
	"bytes"
)

type SelectMappedBufferResult struct {
	startOffset int64
	mappedFile  MappedFile
	byteBuffer  *bytes.Buffer
	size        int32
}
