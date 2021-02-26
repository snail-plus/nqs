package store

import (
	"container/list"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"nqs/common/nutil"
	"nqs/util"
	"os"
	"sort"
	"sync"
)

const mappedFileSize = 1024 * 1024 * 1024

var pathSeparatorStr = "/"
var basePath = util.GetWordDir() + pathSeparatorStr + "store"

type MappedFileQueue struct {
	mappedFileSize int32
	lock           *sync.RWMutex
	mappedFiles    *list.List
	flushedWhere   int64
	storeTimestamp int64
	storePath      string
}

func NewMappedFileQueue(dirName string) *MappedFileQueue {
	storePath := basePath + pathSeparatorStr + dirName
	initDir(storePath)
	log.Infof("storePath: %s", storePath)
	return &MappedFileQueue{
		mappedFileSize: mappedFileSize,
		mappedFiles:    list.New(),
		lock:           new(sync.RWMutex),
		storePath:      storePath,
	}
}

func (r MappedFileQueue) Load() bool {
	path := r.storePath
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Errorf("load dir error, %s", err.Error())
		return false
	}

	var f util.Files = files
	sort.Sort(f)

	for _, onefile := range f {
		if onefile.Size() != int64(r.mappedFileSize) {
			log.Warnf("file: %s length not matched message store config value, please check it manually", onefile.Name())
			return false
		}

		file, err := InitMappedFile(onefile.Name(), r.mappedFileSize)
		if err != nil {
			log.Warnf("InitMappedFile error: %s", err.Error())
			return false
		}

		r.mappedFiles.PushBack(file)
	}

	return true
}

func (r MappedFileQueue) Flush() bool {
	result := true
	mappedFile := r.findMappedFileByOffset(r.flushedWhere, r.flushedWhere == 0)
	if mappedFile == nil {
		return false
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
	defer r.lock.RUnlock()
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
	defer r.lock.RUnlock()

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
	defer r.lock.RUnlock()

	files := r.mappedFiles
	if files.Len() == 0 {
		return nil
	}

	lastFile := files.Back().Value
	return lastFile.(*MappedFile)
}

func (r MappedFileQueue) GetFirstMappedFile() *MappedFile {
	lock := r.lock
	lock.RLock()
	defer lock.RUnlock()

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
		nextFilePath := r.storePath + pathSeparatorStr + nutil.Offset2FileName(createOffset)
		mappedFile, err := InitMappedFile(nextFilePath, int32(r.mappedFileSize))
		if err != nil {
			log.Errorf("InitMappedFile error: %s", err.Error())
			return nil
		}

		lock := r.lock
		lock.Lock()
		r.mappedFiles.PushBack(mappedFile)
		lock.Unlock()
		return mappedFile
	}

	return file
}

func initDir(targetPath string) {
	exists, err := util.PathExists(targetPath)
	if err != nil {
		panic(err)
	}

	if !exists {
		os.MkdirAll(targetPath, 0644)
		log.Infof("initDir %s", targetPath)
	}
}
