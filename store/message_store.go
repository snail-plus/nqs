package store

import (
	"container/list"
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"math"
	"nqs/common"
	"strconv"
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
	GetMessage(group string, topic string, offset int64, queueId, maxMsgNums int32) *GetMessageResult
	DoDispatch(request *DispatchRequest)
}

type DefaultMessageStore struct {
	lock sync.Mutex
	// topic-offset
	topicQueueTable map[string]int64
	//topic-queue
	consumeQueueTable map[string]map[int32]*ConsumeQueue
	commitLog         CommitLog
	stop              bool

	rePutMessageService *RePutMessageService

	dispatcherList *list.List

	flushConsumeQueue FlushConsumeQueue
}

func (r *DefaultMessageStore) Load() bool {
	var result = true
	result = result && r.commitLog.Load()

	result = result && r.loadConsumeQueue()

	if result {
		r.recover()
		log.Info("load done")
	}

	return result
}

func NewStore() *DefaultMessageStore {
	defaultStore := &DefaultMessageStore{}

	defaultStore.commitLog = NewCommitLog(defaultStore)
	defaultStore.topicQueueTable = map[string]int64{}
	defaultStore.consumeQueueTable = map[string]map[int32]*ConsumeQueue{}

	defaultStore.rePutMessageService = &RePutMessageService{commitLog: defaultStore.commitLog, msgStore: defaultStore}

	defaultStore.dispatcherList = list.New()
	defaultStore.dispatcherList.PushBack(CommitLogDispatcherBuildConsumeQueue{store: defaultStore})

	defaultStore.flushConsumeQueue = FlushConsumeQueue{
		store: defaultStore,
	}

	return defaultStore
}

func (r *DefaultMessageStore) Start() {

	r.commitLog.Start()

	// 恢复 rePutMessageService 的reput 指针
	var maxPhysicalPosInLogicQueue = r.commitLog.GetMinOffset()
	for _, maps := range r.consumeQueueTable {
		for _, logic := range maps {
			if logic.maxPhysicOffset > maxPhysicalPosInLogicQueue {
				maxPhysicalPosInLogicQueue = logic.maxPhysicOffset
			}
		}
	}

	if maxPhysicalPosInLogicQueue < 0 {
		maxPhysicalPosInLogicQueue = 0
	}

	r.rePutMessageService.RePutFromOffset = maxPhysicalPosInLogicQueue
	r.rePutMessageService.Start()

	r.flushConsumeQueue.start()

}

func (r *DefaultMessageStore) Shutdown() {
	r.stop = true

	r.commitLog.Shutdown()

	for _, maps := range r.consumeQueueTable {
		for _, logic := range maps {
			logic.Shutdown()
		}
	}

}

func (r *DefaultMessageStore) PutMessages(messageExt *MessageExtBrokerInner) *PutMessageResult {
	result := r.commitLog.PutMessage(messageExt)
	return result
}

func (r *DefaultMessageStore) GetMessage(group string, topic string, offset int64, queueId, maxMsgNums int32) *GetMessageResult {

	getResult := &GetMessageResult{}
	getResult.Status = NoMessageInQueue
	getResult.NextBeginOffset = offset

	consumeQueue := r.findConsumeQueue(topic, queueId)
	if consumeQueue == nil {
		getResult.Status = NoMatchedLogicQueue
		return getResult
	}

	minOffset := consumeQueue.GetMinOffsetInQueue()
	maxOffset := consumeQueue.GetMaxOffsetInQueue()
	getResult.MinOffset = minOffset
	getResult.MaxOffset = maxOffset

	if maxOffset == 0 {
		getResult.Status = NoMessageInQueue
		return getResult
	}

	if offset < minOffset {
		getResult.Status = OffsetTooSmall
		getResult.NextBeginOffset = minOffset
		return getResult
	}

	if offset == maxOffset {
		getResult.Status = OffsetOverflowOne
		getResult.NextBeginOffset = offset
		return getResult
	}

	if offset > maxOffset {
		getResult.Status = OffsetOverflowBadly
		getResult.NextBeginOffset = maxOffset
		return getResult
	}

	// 逻辑Offset
	bufferConsumeQueue := consumeQueue.GetIndexBuffer(offset)
	if bufferConsumeQueue == nil {
		getResult.Status = NoMessageInQueue
		getResult.NextBeginOffset = offset
		return getResult
	}

	var i int32 = 0
	var nextBeginOffset int64 = 0
	buffer := bufferConsumeQueue.ByteBuffer
	maxFilterMessageCount := maxMsgNums * CqStoreUnitSize
	for ; i < bufferConsumeQueue.size && i < maxFilterMessageCount; i += CqStoreUnitSize {

		var offsetPy int64
		binary.Read(buffer, binary.BigEndian, &offsetPy)

		var sizePy int32
		binary.Read(buffer, binary.BigEndian, &sizePy)

		var tagCode int64
		binary.Read(buffer, binary.BigEndian, &tagCode)

		// blank msg skip
		if sizePy == math.MaxInt32 {
			continue
		}

		if sizePy == 0 {
			log.Warnf("[bug] sizePy is 0, offsetPy: %d", offsetPy)
			continue
		}
		// 根据consumeQueue 存储的Offset查找 commitLog存储的消息
		result := r.commitLog.GetMessage(offsetPy, sizePy)

		if result == nil {
			continue
		}

		getResult.AddMessage(result)
		getResult.Status = Found
	}

	nextBeginOffset = offset + (int64(i) / int64(CqStoreUnitSize))
	getResult.NextBeginOffset = nextBeginOffset
	return getResult
}

func (r *DefaultMessageStore) recoverTopicQueueTable() {
	// minPhyOffset := r.commitLog.GetMinOffset()
	for _, maps := range r.consumeQueueTable {
		for _, logic := range maps {
			key := logic.topic + strconv.Itoa(int(logic.queueId))
			topicQueueTable[key] = logic.GetMaxOffsetInQueue()
			log.Infof("recoverTopicQueueTable key: %s, offset: %d", key, logic.GetMaxOffsetInQueue())
			// TODO correctMinOffset
		}
	}
}

func (r *DefaultMessageStore) loadConsumeQueue() bool {
	consumequeuePath := BasePath + "/consumequeue"
	fileTopics, err := ioutil.ReadDir(consumequeuePath)
	if err != nil {
		log.Errorf("read consumequeue dir error: %s", err.Error())
		return false
	}

	for _, fileTopic := range fileTopics {
		topicDir := consumequeuePath + "/" + fileTopic.Name()
		queueDirs, err := ioutil.ReadDir(topicDir)
		if err != nil {
			log.Errorf("read topicDir: %s error: %s", topicDir, err.Error())
			return false
		}

		topic := fileTopic.Name()

		for _, queueDir := range queueDirs {
			queueId, err := strconv.Atoi(queueDir.Name())
			if err != nil {
				log.Warnf("queueName: %s is not int", queueDir.Name())
				continue
			}

			cq := NewConsumeQueue(r, topic, int32(queueId), consumequeuePath)
			if !cq.load() {
				return false
			}
			r.putConsumeQueue(cq)
		}

	}

	return true
}

func (r *DefaultMessageStore) putConsumeQueue(consumeQueue *ConsumeQueue) {
	topic := consumeQueue.topic
	queueId := consumeQueue.queueId

	if item, ok := r.consumeQueueTable[topic]; ok {
		item[queueId] = consumeQueue
	} else {
		r.consumeQueueTable[topic] = map[int32]*ConsumeQueue{queueId: consumeQueue}
	}

}

func (r *DefaultMessageStore) DoDispatch(request *DispatchRequest) {
	dispatcherList := r.dispatcherList
	for item := dispatcherList.Front(); item != nil; item = item.Next() {
		dispatcher := item.Value.(CommitLogDispatcher)
		dispatcher.Dispatch(request)
	}
}

func (r DefaultMessageStore) nextOffsetCorrection(oldOffset, newOffset int64) int64 {
	return newOffset
}

func (r *DefaultMessageStore) recover() {
	maxPhyOffsetConsumeQueue := r.recoverConsumeQueue()
	log.Infof("recover maxPhyOffsetConsumeQueue: %d", maxPhyOffsetConsumeQueue)

	r.commitLog.recoverNormally(maxPhyOffsetConsumeQueue)
	r.recoverTopicQueueTable()
}

func (r *DefaultMessageStore) recoverConsumeQueue() int64 {
	var maxPyOffset int64 = -1
	for _, maps := range r.consumeQueueTable {
		for _, logic := range maps {
			logic.recover()
			if logic.maxPhysicOffset > maxPyOffset {
				maxPyOffset = logic.maxPhysicOffset
			}
		}
	}

	return maxPyOffset
}

func (r *DefaultMessageStore) persist() {
	go func() {
		for !r.stop {

		}
	}()
}

type RePutMessageService struct {
	common.DaemonTask
	RePutFromOffset int64
	commitLog       CommitLog
	msgStore        MessageStore
}

func (r *RePutMessageService) Start() {
	r.DaemonTask.Name = "rePut"
	r.DaemonTask.Run = func() {
		log.Infof("start rePut service,RePutFromOffset: %d", r.RePutFromOffset)
		for !r.IsStopped() {
			time.Sleep(1 * time.Second)
			r.doRePut()
		}
	}

	r.DaemonTask.Start()
}

func (r *RePutMessageService) doRePut() {
	commitLogMinOffset := r.commitLog.GetMinOffset()
	if r.RePutFromOffset < commitLogMinOffset {
		r.RePutFromOffset = commitLogMinOffset
	}

	var index = 0
	for doNext := true; r.isCommitLogAvailable() && doNext; {
		result := r.commitLog.GetData(r.RePutFromOffset, r.RePutFromOffset == 0)
		if result == nil {
			doNext = false
			continue
		}

		if index%100 == 0 {
			log.Infof("doRePut, result, startOffset %d, byte length: %d, size: %d", result.startOffset, result.ByteBuffer.Len(), result.size)
		}

		index++
		r.RePutFromOffset = result.startOffset

		for readSize := 0; readSize < int(result.size) && doNext; {
			dispatchRequest := r.commitLog.CheckMessage(result.ByteBuffer, false, false)
			msgSize := dispatchRequest.msgSize
			if !dispatchRequest.success {
				doNext = false
				continue
			}

			if msgSize == 0 {
				r.RePutFromOffset = r.commitLog.RollNextFile(r.RePutFromOffset)
				readSize = int(result.size)
				continue
			}

			r.msgStore.DoDispatch(dispatchRequest)
			r.RePutFromOffset = r.RePutFromOffset + int64(dispatchRequest.msgSize)
			readSize += int(msgSize)
		}

	}

}

func (r DefaultMessageStore) putMessagePositionInfo(request *DispatchRequest) {
	consumeQueue := r.findConsumeQueue(request.topic, request.queueId)
	consumeQueue.putMessagePositionInfoWrapper(request)
}

func (r *DefaultMessageStore) findConsumeQueue(topic string, queueId int32) *ConsumeQueue {
	// TODO 处理线程安全
	queueMap := r.consumeQueueTable[topic]
	if queueMap == nil {
		queueMap = map[int32]*ConsumeQueue{}
		r.consumeQueueTable[topic] = queueMap
	}

	logic := queueMap[queueId]
	if logic != nil {
		return logic
	}

	cq := NewConsumeQueue(r, topic, queueId, BasePath+"/consumequeue")
	queueMap[queueId] = cq
	return cq
}

func (r RePutMessageService) isCommitLogAvailable() bool {
	return r.RePutFromOffset < r.commitLog.GetMaxOffset()
}

func (r RePutMessageService) Shutdown() {
	r.DaemonTask.Stop()
}
