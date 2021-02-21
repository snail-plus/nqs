package store

import (
	"bytes"
	"nqs/util"
	"strings"
	"sync"
)

type CommitLog struct {
	putMessageLock        sync.RWMutex
	store                 MessageStore
	mappedFileQueue       *MappedFileQueue
	appendMessageCallback AppendMessageCallback
}

func NewCommitLog(store MessageStore) CommitLog {
	c := CommitLog{}
	c.store = store
	c.mappedFileQueue = NewMappedFileQueue()
	c.appendMessageCallback = &DefaultAppendMessageCallback{}
	return c
}

func (r CommitLog) PutMessage(inner *MessageExtBrokerInner) *PutMessageResult {
	r.putMessageLock.Lock()
	defer r.putMessageLock.Unlock()

	messageExt := inner.MessageExt
	messageExt.StoreTimestamp = util.GetUnixTime()

	mappedFile := r.mappedFileQueue.GetLastMappedFile()
	if mappedFile == nil || mappedFile.IsFull() {
		mappedFile = r.mappedFileQueue.GetLastMappedFileByOffset(0, true)
	}

	if mappedFile == nil {
		return &PutMessageResult{
			PutMessageStatus: CreateMappedFileFailed,
		}
	}

	mappedFile.AppendMessage(inner, r.appendMessageCallback)

	return nil
}

type DefaultAppendMessageCallback struct {
	msgIdMemory bytes.Buffer

	msgStoreItemMemory bytes.Buffer
	maxMessageSize     int32
	keyBuilder         strings.Builder
	msgIdBuilder       strings.Builder
}

func (r DefaultAppendMessageCallback) DoAppend(fileFromOffset int64, maxBlank int32, ext *MessageExtBrokerInner) *AppendMessageResult {
	r.msgIdMemory.Write([]byte("a"))
	r.msgIdMemory.Reset()
	return nil
}
