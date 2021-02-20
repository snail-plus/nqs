package store

import (
	"github.com/edsrzf/mmap-go"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"sync/atomic"
)

type MappedFile struct {
	mmap mmap.MMap
	wrotePosition int32
	flushedPosition int32

	fileName string
	fileSize int32
	fileFromOffset int64
	file *os.File
	firstCreateInQueue bool
}


func openFile(filePath string, flags int) (*os.File, error) {
	f, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		log.Errorf("openFile error: %s", err.Error())
		return nil, err
	}
	return f,nil
}

func InitMappedFile(fileName string, fileSize int32) (*MappedFile, error) {

	log.Infof("prepare init mapped file: %s", fileName)
	file, err := openFile(fileName, os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	if err != nil {
		return nil, err
	}

	mmap, err := mmap.MapRegion(file, int(fileSize), mmap.RDWR, 0,0)
	if err != nil {
		log.Errorf("mmap error: %s", err.Error())
		return nil, err
	}

	fileFromOffset, err := strconv.ParseInt(file.Name(), 10, 64)
	if err != nil {
		return nil,err
	}

	return &MappedFile{
		mmap:               mmap,
		wrotePosition:      0,
		flushedPosition:    0,
		fileName:           file.Name(),
		fileSize:           fileSize,
		fileFromOffset:     fileFromOffset,
		file:               file,
		firstCreateInQueue: false,
	},nil

}

func (r MappedFile) AppendMessage(msg *MessageExtBrokerInner, callback AppendMessageCallback) *AppendMessageResult {
    return r.AppendMessageInner(msg, callback)
}

func (r MappedFile) AppendMessageInner(msg *MessageExtBrokerInner, callback AppendMessageCallback) *AppendMessageResult {

	currentPos := atomic.LoadInt32(&r.wrotePosition)
	if currentPos < r.fileSize {
        callback.DoAppend(r.fileFromOffset, r.fileSize - currentPos, msg)
	}

	r.mmap[9] = '0'
	// 写入消息到文件
	copy(r.mmap[4:8], []byte("a"))
	return nil
}

func (r MappedFile) IsFull() bool {
	return  atomic.LoadInt32(&r.wrotePosition) == r.fileSize
}