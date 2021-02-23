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
	lock           sync.Mutex
	mappedFiles    *list.List
	flushedWhere   int64
	storeTimestamp int64
}

func NewMappedFileQueue() *MappedFileQueue {
	initDir()
	return &MappedFileQueue{
		mappedFileSize: mappedFileSize,
		mappedFiles:    list.New(),
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
	return nil
}

func (r MappedFileQueue) GetLastMappedFile() *MappedFile {
	files := r.mappedFiles
	if files.Len() == 0 {
		return nil
	}

	lastFile := files.Back().Value
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

		r.mappedFiles.PushBack(mappedFile)
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
