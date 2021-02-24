package store

import (
	"container/list"
	log "github.com/sirupsen/logrus"
	"nqs/common/nutil"
	"nqs/util"
	"os"
	"sync"
)

const mappedFileSize = 1024 * 1024 * 1024

var pathSeparatorStr = "/"
var storePath = util.GetWordDir() + pathSeparatorStr + "store"

type MappedFileQueue struct {
	mappedFileSize int
	lock           *sync.RWMutex
	mappedFiles    *list.List
	flushedWhere   int64
	storeTimestamp int64
}

func NewMappedFileQueue() *MappedFileQueue {
	initDir()
	return &MappedFileQueue{
		mappedFileSize: mappedFileSize,
		mappedFiles:    list.New(),
		lock:           new(sync.RWMutex),
	}
}

func (r MappedFileQueue) Flush() bool {
	result := true
	mappedFile := r.findMappedFileByOffset(r.flushedWhere, r.flushedWhere == 0)
	if mappedFile == nil {
		return result
	}

	tmpTimeStamp := mappedFile.storeTimestamp
	flushedPosition := mappedFile.Flush()
	where := mappedFile.fileFromOffset + int64(flushedPosition)
	result = where == r.flushedWhere

	r.flushedWhere = where
	r.storeTimestamp = tmpTimeStamp

	return result
}

func (r MappedFileQueue) findMappedFileByOffset(offset int64, returnFirstOnNotFound bool) *MappedFile {
	firstMappedFile := r.GetFirstMappedFile()
	lastMappedFile := r.GetLastMappedFile()
	if firstMappedFile == nil || lastMappedFile == nil {
		return nil
	}

	if offset < firstMappedFile.fileFromOffset || offset >= lastMappedFile.fileFromOffset+int64(r.mappedFileSize) {
		if returnFirstOnNotFound {
			return firstMappedFile
		}

		return nil
	}

	validateFileFn := func(offset int64, mappedFile *MappedFile) bool {
		return offset >= mappedFile.fileFromOffset && offset < mappedFile.fileFromOffset+int64(r.mappedFileSize)
	}

	index := offset/int64(r.mappedFileSize) - firstMappedFile.fileFromOffset/int64(r.mappedFileSize)
	targetFile := r.getMappedFileByIndex(int(index))
	if targetFile != nil && validateFileFn(offset, targetFile) {
		return targetFile
	}

	r.lock.RLock()
	defer r.lock.Unlock()
	files := r.mappedFiles
	for item := files.Front(); item != nil; item = item.Next() {
		tmpFile := item.Value.(*MappedFile)
		if validateFileFn(offset, tmpFile) {
			return tmpFile
		}
	}

	if returnFirstOnNotFound {
		return firstMappedFile
	}

	return nil
}

func (r MappedFileQueue) getMappedFileByIndex(index int) *MappedFile {
	r.lock.RLock()
	defer r.lock.Unlock()
	if r.mappedFiles.Len() == 0 {
		return nil
	}

	i := 0
	files := r.mappedFiles
	for item := files.Front(); item != nil; item = item.Next() {
		if i == index {
			return item.Value.(*MappedFile)
		}
		i++
	}

	return nil

}

func (r MappedFileQueue) GetLastMappedFile() *MappedFile {
	r.lock.RLock()
	defer r.lock.Unlock()

	files := r.mappedFiles
	if files.Len() == 0 {
		return nil
	}

	lastFile := files.Back().Value
	return lastFile.(*MappedFile)
}

func (r MappedFileQueue) GetFirstMappedFile() *MappedFile {
	r.lock.RLock()
	defer r.lock.Unlock()

	files := r.mappedFiles
	if files.Len() == 0 {
		return nil
	}

	lastFile := files.Front().Value
	return lastFile.(*MappedFile)
}

func (r MappedFileQueue) GetLastMappedFileByOffset(startOffset int64, needCreate bool) *MappedFile {
	var createOffset int64 = -1
	file := r.GetLastMappedFile()
	if file == nil {
		createOffset = startOffset - (startOffset % int64(r.mappedFileSize))
	} else if file != nil && file.IsFull() {
		createOffset = file.fileFromOffset + int64(r.mappedFileSize)
	}

	if createOffset != -1 && needCreate {
		nextFilePath := storePath + pathSeparatorStr + nutil.Offset2FileName(createOffset)
		mappedFile, err := InitMappedFile(nextFilePath, int32(r.mappedFileSize))
		if err != nil {
			log.Errorf("InitMappedFile error: %s", err.Error())
			return nil
		}

		r.lock.Lock()
		r.mappedFiles.PushBack(mappedFile)
		r.lock.Unlock()
		return mappedFile
	}

	return file
}

func initDir() {
	exists, err := util.PathExists(storePath)
	if err != nil {
		panic(err)
	}

	if !exists {
		os.MkdirAll(storePath, 0644)
		log.Infof("initDir %s", storePath)
	}
}
