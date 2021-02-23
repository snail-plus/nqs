package store

import (
	log "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	lutil "github.com/syndtr/goleveldb/leveldb/util"
	"nqs/common/message"
	"nqs/util"
	"os"
	"path/filepath"
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
}

func (r *DefaultMessageStore) Load() bool {
	return true
}

func (r *DefaultMessageStore) Start() {
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	runPath := strings.Replace(dir, "\\", "/", -1)
	log.Infof("runPath: %s", runPath)

	fileDb, err := leveldb.OpenFile(runPath+"/db", nil)
	if err != nil {
		panic(err)
	}

	r.db = fileDb
	r.commitLog = NewCommitLog(r)
	r.commitLog.Start()

	r.recoverTopicQueueTable()
	r.topicQueueTable = map[string]int64{}
	r.consumeQueueTable = map[string]string{}

	r.persist()
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

	iter := r.db.NewIterator(lutil.BytesPrefix([]byte(topicOffsetPrefix)), nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		topic := strings.TrimPrefix(string(key), topicOffsetPrefix)
		r.topicQueueTable[topic] = util.BytesToInt64(value)
	}

	log.Infof("topicQueueTable: %v", r.topicQueueTable)
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
