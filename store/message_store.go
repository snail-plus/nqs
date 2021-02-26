package store

import (
	log "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	lutil "github.com/syndtr/goleveldb/leveldb/util"
	"nqs/common"
	"nqs/common/message"
	"nqs/util"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	topicOffsetPrefix = "to"
)

type MessageStore interface {
	Load() bool
	Start()
	Shutdown()
	PutMessages(*MessageExtBrokerInner) *PutMessageResult
	GetMessage(group string, topic string, offset int64, maxMsgNums int32) *GetMessageResult
}

type DefaultMessageStore struct {
	lock sync.Mutex
	db   *leveldb.DB
	// topic-offset
	topicQueueTable map[string]int64
	//topic-queue
	consumeQueueTable map[string]string
	commitLog         CommitLog
	stop              bool

	rePutMessageService RePutMessageService
}

func (r *DefaultMessageStore) Load() bool {
	r.commitLog.Load()
	return true
}

func NewStore() *DefaultMessageStore {
	defaultStore := &DefaultMessageStore{}

	defaultStore.commitLog = NewCommitLog(defaultStore)
	defaultStore.topicQueueTable = map[string]int64{}
	defaultStore.consumeQueueTable = map[string]string{}

	defaultStore.rePutMessageService = RePutMessageService{commitLog: defaultStore.commitLog}
	return defaultStore
}

func (r *DefaultMessageStore) Start() {

	r.commitLog.Start()

	r.rePutMessageService.Start()

	r.recoverTopicQueueTable()

}

func (r *DefaultMessageStore) Shutdown() {
	r.stop = true

	r.commitLog.Shutdown()
}

func (r *DefaultMessageStore) PutMessages(messageExt *MessageExtBrokerInner) *PutMessageResult {
	result := r.commitLog.PutMessage(messageExt)
	return result
}

func (r *DefaultMessageStore) GetMessage(group string, topic string, offset int64, maxMsgNums int32) *GetMessageResult {
	r.lock.Lock()
	defer r.lock.Unlock()

	// 存储格式 key: topic-offset value: 消息值
	baseOffsetStr := topic + strconv.FormatInt(offset, 10)
	iter := r.db.NewIterator(&lutil.Range{
		Start: []byte(baseOffsetStr),
		Limit: []byte(baseOffsetStr + strconv.Itoa(int(maxMsgNums))),
	}, nil)
	defer iter.Release()

	ext := make([]*message.MessageExt, 0, maxMsgNums)

	index := 0
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		value := iter.Value()

		keyOffset, err := strconv.ParseInt(strings.TrimPrefix(string(key), topic), 10, 64)
		if err != nil {
			log.Errorf("key 提取offset失败, key: %s, group: %s", string(key), group)
			continue
		}

		ext[index] = &message.MessageExt{
			BrokerName:    "",
			BornTimestamp: time.Now().Unix(),
			QueueOffset:   keyOffset,
			Message: message.Message{
				Topic: topic,
				Body:  value},
		}
		index++
	}

	if index == 0 {
		return &GetMessageResult{Status: NoMatchedMessage, Messages: ext[0:index]}
	}

	getResult := &GetMessageResult{Status: Found, Messages: ext[0:index]}

	return getResult
}

func (r *DefaultMessageStore) recoverTopicQueueTable() {
	r.topicQueueTable = map[string]int64{}

}

func (r *DefaultMessageStore) persist() {
	go func() {
		var index int64 = 0
		for !r.stop {
			for k, v := range r.topicQueueTable {
				err := r.db.Put([]byte(topicOffsetPrefix+k), util.Int64ToBytes(v), nil)
				if err != nil {
					log.Errorf("persist topicQueueTable error, %s", err.Error())
				}

				if index%20 == 0 {
					log.Infof("k: %s, v: %d, persist success", k, v)
				}

				time.Sleep(5 * time.Second)
				index++
			}
		}
	}()
}

type RePutMessageService struct {
	common.DaemonTask
	RePutFromOffset int64
	commitLog       CommitLog
}

func (r RePutMessageService) Start() {
	r.DaemonTask.Name = "rePut"
	r.DaemonTask.Run = func() {
		for !r.IsStopped() {
			time.Sleep(1 * time.Second)
			r.doRePut()
		}
	}

	r.DaemonTask.Start()
}

func (r RePutMessageService) doRePut() {
	commitLogMinOffset := r.commitLog.GetMinOffset()
	if r.RePutFromOffset < commitLogMinOffset {
		r.RePutFromOffset = commitLogMinOffset
	}

	for doNext := true; r.isCommitLogAvailable() && doNext; {

	}

}

func (r RePutMessageService) isCommitLogAvailable() bool {
	return r.RePutFromOffset < r.commitLog.GetMaxOffset()
}

func (r RePutMessageService) Shutdown() {
	r.DaemonTask.Stop()
}
