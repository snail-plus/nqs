package buffer

import (
	"bytes"
	"sync"
)

type BufPool interface {
	GetBuffer() *bytes.Buffer
	PutBuffer(*bytes.Buffer)
}

type SyncBufPool struct {
	sync.Pool
}

func NewDefaultBufPool() BufPool {
	return &SyncBufPool{
		sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

func (r *SyncBufPool) GetBuffer() *bytes.Buffer {
	return r.Get().(*bytes.Buffer)
}

func (r *SyncBufPool) PutBuffer(buf *bytes.Buffer) {
	buf.Reset()
	r.Put(buf)
}
