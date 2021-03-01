package store

import (
	"bytes"
)

type SelectMappedBufferResult struct {
	startOffset int64
	mappedFile  *MappedFile
	ByteBuffer  *bytes.Buffer
	size        int32
}
