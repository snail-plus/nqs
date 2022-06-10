package common

import (
	"bytes"
	"sync"
)

var headerPool = sync.Pool{}
var bufferPool = sync.Pool{}

func init() {
	headerPool.New = func() interface{} {
		return make([]byte, 4)
	}
	bufferPool.New = func() interface{} {
		return new(bytes.Buffer)
	}
}

func GetHeader() []byte {
	d := headerPool.Get().([]byte)
	return d
}

func BackHeader(d []byte) {
	for i := range d {
		d[i] = 0
	}
	headerPool.Put(d)
}

func GetBuffer() *bytes.Buffer {
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func BackBuffer(b *bytes.Buffer) {
	b.Reset()
	bufferPool.Put(b)
}
