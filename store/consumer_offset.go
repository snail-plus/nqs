package store

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"nqs/common"
	"sync"
)

const TopicGroupSeparator = "@"

type ConsumerOffsetManager struct {
	common.ConfigManager `json:"-"`
	lock                 *sync.RWMutex              `json:"-"`
	OffsetTable          map[string]map[int32]int64 `json:"OffsetTable"`
}

func (r *ConsumerOffsetManager) ConfigFilePath() string {
	return BasePath + "/config" + "/" + "consumerOffset.json"
}

func (r *ConsumerOffsetManager) Encode() string {
	marshal, err := json.Marshal(&r)
	if err != nil {
		log.Errorf("Encode error: %s", err.Error())
		return ""
	}
	return string(marshal)
}

func (r *ConsumerOffsetManager) Decode(jsonString string) {
	tmpConsumerOffsetManager := &ConsumerOffsetManager{}
	err := json.Unmarshal([]byte(jsonString), tmpConsumerOffsetManager)
	if err != nil {
		log.Errorf("Unmarshal error: %s", err.Error())
		return
	}

	r.OffsetTable = tmpConsumerOffsetManager.OffsetTable
	log.Infof("OffsetTable: %+v", r.OffsetTable)
}

func (r *ConsumerOffsetManager) CommitOffset(clientHost, group, topic string, queueId int32, offset int64) {
	r.lock.Lock()
	defer r.lock.Unlock()

	key := topic + TopicGroupSeparator + group
	offsetMap, ok := r.OffsetTable[key]

	if !ok {
		tmpOffsetMap := map[int32]int64{queueId: offset}
		r.OffsetTable[key] = tmpOffsetMap
	} else {
		offsetMap[queueId] = offset
	}

	log.Infof("CommitOffset client: %s, offset: %d, OffsetTable: %v, %p", clientHost, offset, r.OffsetTable, r)
}

func (r *ConsumerOffsetManager) QueryOffset(group, topic string, queueId int32) int64 {
	r.lock.RLock()
	defer r.lock.RUnlock()

	key := topic + TopicGroupSeparator + group
	offsetMap, ok := r.OffsetTable[key]
	if !ok {
		return -1
	}

	offset, exist := offsetMap[queueId]
	if !exist {
		return -1
	}

	return offset
}

func NewConsumerOffsetManager() *common.ConfigManager {
	var configManager = &common.ConfigManager{}
	configManager.Config = &ConsumerOffsetManager{
		lock:        new(sync.RWMutex),
		OffsetTable: make(map[string]map[int32]int64),
	}

	return configManager
}
