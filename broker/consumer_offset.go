package broker

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"nqs/common"
	"nqs/store"
	"sync"
)

const TopicGroupSeparator = "@"

type ConsumerOffsetManager struct {
	common.ConfigManager `json:"-"`
	lock                 *sync.RWMutex              `json:"-"`
	OffsetTable          map[string]map[int32]int64 `json:"OffsetTable"`
}

func (r *ConsumerOffsetManager) ConfigFilePath() string {
	return store.BasePath + "/config" + "/" + "consumerOffset.json"
}

func (r *ConsumerOffsetManager) Encode() string {
	marshal, err := json.Marshal(r)
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

	log.Infof("CommitOffset client: %s, offset: %d", clientHost, offset)
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

func NewConsumerOffsetManager() *ConsumerOffsetManager {
	consumerOffsetManager := new(ConsumerOffsetManager)
	consumerOffsetManager.lock = new(sync.RWMutex)
	consumerOffsetManager.OffsetTable = map[string]map[int32]int64{}

	consumerOffsetManager.ConfigManager.Decode = consumerOffsetManager.Decode
	consumerOffsetManager.ConfigManager.Encode = consumerOffsetManager.Encode
	consumerOffsetManager.ConfigManager.ConfigFilePath = consumerOffsetManager.ConfigFilePath
	return consumerOffsetManager
}
