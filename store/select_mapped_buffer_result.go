package store

import "github.com/edsrzf/mmap-go"

type SelectMappedBufferResult struct {
	startOffset int64
	mappedFile  MappedFile
	mmap        mmap.MMap
	size        int32
}
