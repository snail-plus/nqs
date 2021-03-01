package store

import (
	"container/list"
	"encoding/binary"
	log "github.com/sirupsen/logrus"
	"nqs/common"
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

	rePutMessageService RePutMessageService

	dispatcherList *list.List

	flushConsumeQueue FlushConsumeQueue
}

func (r *DefaultMessageStore) Load() bool {
	r.commitLog.Load()
	return true
}

func NewStore() *DefaultMessageStore {
	defaultStore := &DefaultMessageStore{}

	defaultStore.commitLog = NewCommitLog(defaultStore)
	defaultStore.topicQueueTable = map[string]int64{}
	defaultStore.consumeQueueTable = map[string]map[int32]*ConsumeQueue{}

	defaultStore.rePutMessageService = RePutMessageService{commitLog: defaultStore.commitLog, msgStore: defaultStore}

	defaultStore.dispatcherList = list.New()
	defaultStore.dispatcherList.PushBack(CommitLogDispatcherBuildConsumeQueue{store: defaultStore})

	defaultStore.flushConsumeQueue = FlushConsumeQueue{
		store: defaultStore,
	}

	return defaultStore
}

func (r *DefaultMessageStore) Start() {

	r.commitLog.Start()

	r.rePutMessageService.Start()

	r.flushConsumeQueue.start()

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

func (r *DefaultMessageStore) GetMessage(group string, topic string, offset int64, queueId, maxMsgNums int32) *GetMessageResult {

	getResult := &GetMessageResult{}
	getResult.Status = OffsetFoundNull

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
		getResult.Status = OffsetFoundNull
		return getResult
	}

	var i int32 = 0
	var nextBeginOffset int64 = 0
	buffer := bufferConsumeQueue.byteBuffer
	log.Infof("buffer length: %d", buffer.Len())
	for ; i < bufferConsumeQueue.size && i < maxMsgNums; i += CqStoreUnitSize {

		var offsetPy int64
		binary.Read(buffer, binary.BigEndian, offsetPy)

		var sizePy int32
		binary.Read(buffer, binary.BigEndian, sizePy)

		var tagCode int64
		binary.Read(buffer, binary.BigEndian, tagCode)

		if sizePy == 0 {
			log.Warnf("[bug] sizePy is 0, offsetPy: %d", offsetPy)
			continue
		}
		// 根据consumeQueue 存储的Offset查找 commitLog存储的消息
		log.Infof("xxxx1")
		result := r.commitLog.GetMessage(offsetPy, sizePy)
		log.Infof("xxxx2")

		if result == nil {
			continue
		}

		nextBeginOffset = offset + (int64(i) / int64(CqStoreUnitSize))
		getResult.AddMessage(result)
		getResult.Status = Found
		getResult.NextBeginOffset = nextBeginOffset
	}

	return getResult
}

func (r *DefaultMessageStore) recoverTopicQueueTable() {
	r.topicQueueTable = map[string]int64{}
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

func (r RePutMessageService) Start() {
	r.DaemonTask.Name = "rePut"
	r.DaemonTask.Run = func() {
		log.Infof("start rePut service")
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

	for doNext := true; r.isCommitLogAvailable() && doNext; {
		result := r.commitLog.GetData(r.RePutFromOffset, r.RePutFromOffset == 0)
		if result == nil {
			doNext = false
			continue
		}

		r.RePutFromOffset = result.startOffset
		log.Infof("result, startOffset %d, byte length: %d, size: %d", result.startOffset, result.byteBuffer.Len(), result.size)

		for readSize := 0; readSize < int(result.size) && doNext; {
			dispatchRequest := r.commitLog.CheckMessage(result.byteBuffer, false, false)
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
			log.Infof("RePutFromOffset %d", r.RePutFromOffset)
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
