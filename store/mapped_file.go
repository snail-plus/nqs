package store

import (
	"bytes"
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

	log.Infof("prepare init mapped file: %s, fileSize: %d", fileName, fileSize)
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

func (r *MappedFile) AppendMessage(msg *MessageExtBrokerInner, callback AppendMessageCallback) *AppendMessageResult {
	return r.AppendMessageInner(msg, callback)
}

func (r *MappedFile) AppendMessageBytes(data []byte) bool {
	currentPos := atomic.LoadInt32(&r.wrotePosition)
	if currentPos+int32(len(data)) <= r.fileSize {
		copy(r.mmap[currentPos:], data)
		r.wrotePosition = atomic.AddInt32(&r.wrotePosition, int32(len(data)))
		return true
	}

	return false
}

func (r *MappedFile) AppendMessageInner(msg *MessageExtBrokerInner, callback AppendMessageCallback) *AppendMessageResult {

	var appendMessageResult *AppendMessageResult
	currentPos := atomic.LoadInt32(&r.wrotePosition)
	if currentPos < r.fileSize {
		appendMessageResult = callback.DoAppend(r.MappedByte(currentPos), currentPos, r.fileFromOffset, r.fileSize-currentPos, msg)
		r.wrotePosition = atomic.AddInt32(&r.wrotePosition, appendMessageResult.WroteBytes)
		log.Infof("wrotePosition %d", r.wrotePosition)
		r.storeTimestamp = util.GetUnixTime()
		return appendMessageResult
	}

	appendMessageResult = &AppendMessageResult{
		Status: AppendUnknownError,
	}

	return appendMessageResult
}

func (r *MappedFile) IsFull() bool {
	return atomic.LoadInt32(&r.wrotePosition) == r.fileSize
}

/**
  return The current flushed position
*/
func (r *MappedFile) Flush() int32 {
	value := atomic.LoadInt32(&r.wrotePosition)

	if r.isAbleToFlush() {
		err := r.mmap.Flush()
		r.flushedPosition = value
		if err != nil {
			log.Errorf("mmap flush error: %d", err.Error())
		}
		log.Infof("flush pos is %d", value)
		return value
	}

	return atomic.LoadInt32(&r.flushedPosition)
}

func (r *MappedFile) isAbleToFlush() bool {
	if r.IsFull() {
		return true
	}

	flush := atomic.LoadInt32(&r.flushedPosition)
	write := atomic.LoadInt32(&r.wrotePosition)
	return write > flush
}

func (r *MappedFile) MappedByte(position int32) mmap.MMap {
	return r.mmap[position:]
}

func (r MappedFile) GetWrotePosition() int32 {
	return atomic.LoadInt32(&r.wrotePosition)
}

func (r MappedFile) GetFlushedPosition() int32 {
	return atomic.LoadInt32(&r.flushedPosition)
}

func (r *MappedFile) selectMappedBuffer(pos int32) *SelectMappedBufferResult {
	readPosition := atomic.LoadInt32(&r.wrotePosition)

	if pos < readPosition && pos >= 0 {
		size := readPosition - pos
		return &SelectMappedBufferResult{
			startOffset: r.fileFromOffset + int64(pos),
			mappedFile:  r,
			ByteBuffer:  bytes.NewBuffer(r.mmap[pos : pos+size]),
			size:        size,
		}
	}

	return nil
}

func (r *MappedFile) selectMappedBufferBySize(pos, size int32) *SelectMappedBufferResult {
	readPosition := atomic.LoadInt32(&r.wrotePosition)

	if pos+size <= readPosition {
		return &SelectMappedBufferResult{
			startOffset: r.fileFromOffset + int64(pos),
			mappedFile:  r,
			ByteBuffer:  bytes.NewBuffer(r.mmap[pos : pos+size]),
			size:        size,
		}
	}

	return nil
}
