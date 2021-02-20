package store

import (
  "nqs/util"
  "sync"
)

type CommitLog struct {
  putMessageLock sync.RWMutex
  store MessageStore
  mappedFileQueue MappedFileQueue
  appendMessageCallback AppendMessageCallback
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