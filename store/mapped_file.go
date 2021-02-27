package store

import (
	"github.com/edsrzf/mmap-go"
	log "github.com/sirupsen/logrus"
	"nqs/util"
	"os"
	"strconv"
	"sync/atomic"
)

type MappedFile struct {
	mmap            mmap.MMap
	wrotePosition   int32
	flushedPosition int32
	storeTimestamp  int64

	fileName           string
	fileSize           int32
	fileFromOffset     int64
	file               *os.File
	firstCreateInQueue bool
}

func openFile(filePath string, flags int) (*os.File, error) {
	f, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		log.Errorf("openFile error: %s", err.Error())
		return nil, err
	}
	return f, nil
}

func InitMappedFile(fileName string, fileSize int32) (*MappedFile, error) {

	log.Infof("prepare init mapped file: %s", fileName)
	file, err := openFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return nil, err
	}

	mmap, err := mmap.MapRegion(file, int(fileSize), mmap.RDWR, 0, 0)
	if err != nil {
		log.Errorf("mmap error: %s", err.Error())
		return nil, err
	}

	fileShortName := util.GetFileNameByFullPath(fileName)
	fileFromOffset, err := strconv.ParseInt(fileShortName, 10, 64)
	if err != nil {
		log.Errorf("ParseInt fileName error, fileName: %s ", fileShortName)
		return nil, err
	}

	log.Infof("mmap len: %d, cap: %d", len(mmap), cap(mmap))
	return &MappedFile{
		mmap:               mmap,
		wrotePosition:      0,
		flushedPosition:    0,
		fileName:           file.Name(),
		fileSize:           fileSize,
		fileFromOffset:     fileFromOffset,
		file:               file,
		firstCreateInQueue: false,
	}, nil

}

func (r MappedFile) AppendMessage(msg *MessageExtBrokerInner, callback AppendMessageCallback) *AppendMessageResult {
	return r.AppendMessageInner(msg, callback)
}

func (r MappedFile) AppendMessageInner(msg *MessageExtBrokerInner, callback AppendMessageCallback) *AppendMessageResult {

	var appendMessageResult *AppendMessageResult
	currentPos := atomic.LoadInt32(&r.wrotePosition)
	if currentPos < r.fileSize {
		appendMessageResult = callback.DoAppend(r.MappedByte(currentPos), currentPos, r.fileFromOffset, r.fileSize-currentPos, msg)
		r.wrotePosition = atomic.AddInt32(&r.wrotePosition, appendMessageResult.WroteBytes)
		r.storeTimestamp = util.GetUnixTime()
		return appendMessageResult
	}

	appendMessageResult = &AppendMessageResult{
		Status: AppendUnknownError,
	}

	return appendMessageResult
}

func (r MappedFile) IsFull() bool {
	return atomic.LoadInt32(&r.wrotePosition) == r.fileSize
}

/**
  return The current flushed position
*/
func (r MappedFile) Flush() int32 {
	return 0
}

func (r MappedFile) MappedByte(position int32) mmap.MMap {
	return r.mmap[position:]
}

func (r MappedFile) selectMappedBuffer(pos int32) *SelectMappedBufferResult {
	readPosition := atomic.LoadInt32(&r.wrotePosition)

	if pos < readPosition && pos >= 0 {
		size := readPosition - pos
		return &SelectMappedBufferResult{
			startOffset: r.fileFromOffset + int64(pos),
			mappedFile:  MappedFile{},
			mmap:        r.mmap[pos:size],
			size:        size,
		}
	}

	return nil
}
