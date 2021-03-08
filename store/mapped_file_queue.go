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
	"sync/atomic"
)

const commitLogFileSize = 1024 * 1024 * 1024
const mappedFileSizeConsumeQueue = 300000 * CqStoreUnitSize

var pathSeparatorStr = "/"
var BasePath = util.GetWordDir() + pathSeparatorStr + "store"

type MappedFileQueue struct {
	mappedFileSize int32
	lock           *sync.RWMutex
	mappedFiles    *list.List
	flushedWhere   int64
	storeTimestamp int64
	storePath      string
}

func NewMappedFileQueue(path string, mappedFileSize int) *MappedFileQueue {
	initDir(path)
	log.Infof("storePath: %s", path)
	return &MappedFileQueue{
		mappedFileSize: int32(mappedFileSize),
		mappedFiles:    list.New(),
		lock:           new(sync.RWMutex),
		storePath:      path,
	}
}

func (r *MappedFileQueue) Load() bool {
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

		file, err := InitMappedFile(path+"/"+onefile.Name(), r.mappedFileSize)
		if err != nil {
			log.Warnf("InitMappedFile error: %s", err.Error())
			return false
		}

		file.wrotePosition = r.mappedFileSize
		file.flushedPosition = r.mappedFileSize
		r.mappedFiles.PushBack(file)
	}

	lastFile := r.mappedFiles.Back()
	if lastFile != nil {
		mappedFile := lastFile.Value.(*MappedFile)
		mappedFile.warmMappedFile()
	}

	return true
}

func (r *MappedFileQueue) DeleteLastMappedFile() {
	r.lock.Lock()
	defer r.lock.Unlock()

	last := r.mappedFiles.Back()
	if last == nil {
		return
	}

	mappedFile := last.Value.(*MappedFile)
	mappedFile.destroy()
	r.mappedFiles.Remove(last)
}

func (r *MappedFileQueue) Flush() bool {
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

func (r *MappedFileQueue) findMappedFileByOffset(offset int64, returnFirstOnNotFound bool) *MappedFile {
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

func (r *MappedFileQueue) GetLastMappedFile() *MappedFile {
	r.lock.RLock()
	defer r.lock.RUnlock()

	files := r.mappedFiles
	if files.Len() == 0 {
		return nil
	}

	lastFile := files.Back().Value
	return lastFile.(*MappedFile)
}

func (r *MappedFileQueue) GetFirstMappedFile() *MappedFile {
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

func (r *MappedFileQueue) GetMaxOffset() int64 {
	r.lock.RLock()
	defer r.lock.RUnlock()

	file := r.GetLastMappedFile()
	if file == nil {
		return 0
	}
	currentPos := atomic.LoadInt32(&file.wrotePosition)
	return file.fileFromOffset + int64(currentPos)
}

func (r *MappedFileQueue) GetMinOffset() int64 {
	r.lock.RLock()
	defer r.lock.RUnlock()

	file := r.GetFirstMappedFile()
	if file == nil {
		return -1
	}

	return file.fileFromOffset
}

func (r *MappedFileQueue) GetLastMappedFileByOffset(startOffset int64, needCreate bool) *MappedFile {
	var createOffset int64 = -1
	file := r.GetLastMappedFile()
	if file == nil {
		createOffset = startOffset - (startOffset % int64(r.mappedFileSize))
	} else if file != nil && file.IsFull() {
		createOffset = file.fileFromOffset + int64(r.mappedFileSize)
	}

	if createOffset != -1 && needCreate {
		nextFilePath := r.storePath + pathSeparatorStr + nutil.Offset2FileName(createOffset)
		mappedFile, err := InitMappedFile(nextFilePath, r.mappedFileSize)
		if err != nil {
			log.Errorf("InitMappedFile error: %s", err.Error())
			return nil
		}

		lock := r.lock
		lock.Lock()
		if r.mappedFiles.Len() == 0 {
			mappedFile.firstCreateInQueue = true
		}

		r.mappedFiles.PushBack(mappedFile)
		lock.Unlock()
		return mappedFile
	}

	return file
}

func (r *MappedFileQueue) Shutdown() {
	for item := r.mappedFiles.Front(); item != nil; item = item.Next() {
		mappedFile := item.Value.(*MappedFile)
		mappedFile.Shutdown()
	}
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
