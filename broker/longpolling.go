package broker

import (
	"container/list"
	"fmt"
	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"nqs/broker/longpolling"
	"nqs/code"
	"nqs/common"
	"nqs/processor"
	"nqs/store"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TopicQueueIdSeparator = "@"
)

type ManyPullRequest struct {
	lock            sync.Mutex
	pullRequestList *list.List // []*PullRequest
}

func (r *ManyPullRequest) cloneListAndClear() *list.List {
	r.lock.Lock()
	defer r.lock.Unlock()

	tmpPullRequestList := list.New()

	if r.pullRequestList == nil || r.pullRequestList.Len() == 0 {
		return tmpPullRequestList
	}

	for item := r.pullRequestList.Front(); item != nil; item = item.Next() {
		tmpPullRequestList.PushBack(item.Value)
	}

	r.pullRequestList.Init()

	return tmpPullRequestList
}

type PullRequestHoldService struct {
	common.DaemonTask
	msgStore store.MessageStore

	pullRequestTable sync.Map //map[string]*ManyPullRequest
	pool             *ants.Pool
}

func NewPullRequestHoldService(msgStore store.MessageStore) longpolling.LongPolling {
	pool, _ := ants.NewPool(5, ants.WithPreAlloc(true))
	return &PullRequestHoldService{
		msgStore: msgStore,
		pool:     pool,
	}
}

func (r *PullRequestHoldService) Start() {
	log.Infof("start up PullRequestHoldService")
	r.DaemonTask.Name = "PullRequestHoldService"
	r.DaemonTask.Run = func() {
		for !r.IsStopped() {
			time.Sleep(1 * time.Second)
			r.checkHoldRequest()
		}
	}

	r.DaemonTask.Start()
}

func (r *PullRequestHoldService) checkHoldRequest() {

	r.pullRequestTable.Range(func(key, value interface{}) bool {
		split := strings.Split(key.(string), TopicQueueIdSeparator)
		topic := split[0]
		queueId, _ := strconv.Atoi(split[1])
		maxOffset := r.msgStore.GetMaxOffsetInQueue(topic, int32(queueId))
		r.NotifyMessageArriving(topic, queueId, maxOffset)
		return true
	})

}

func (r *PullRequestHoldService) SuspendPullRequest(topic string, queueId int32, pullRequest *longpolling.PullRequest) {
	key := topic + TopicQueueIdSeparator + fmt.Sprintf("%d", queueId)

	manyPullRequest := &ManyPullRequest{pullRequestList: list.New()}

	actual, _ := r.pullRequestTable.LoadOrStore(key, manyPullRequest)
	real := actual.(*ManyPullRequest)
	real.pullRequestList.PushBack(pullRequest)
}

func (r *PullRequestHoldService) NotifyMessageArriving(topic string, queueId int, maxOffset int64) {

	key := topic + TopicQueueIdSeparator + fmt.Sprintf("%d", queueId)
	manyPullRequest, ok := r.pullRequestTable.Load(key)
	if !ok {
		return
	}

	pullRequestList := manyPullRequest.(*ManyPullRequest).cloneListAndClear()
	if pullRequestList.Len() == 0 {
		return
	}

	replayList := list.New()

	for item := pullRequestList.Front(); item != nil; item = item.Next() {
		newestOffset := maxOffset
		pullRequest := item.Value.(*longpolling.PullRequest)

		if newestOffset <= pullRequest.PullFromThisOffset {
			newestOffset = r.msgStore.GetMaxOffsetInQueue(topic, int32(queueId))
		}

		// 有新消息
		command := pullRequest.RequestCommand
		if newestOffset > pullRequest.PullFromThisOffset {
			var suspendTimeoutMillis int64 = 0
			command.ExtFields["SuspendTimeoutMillis"] = suspendTimeoutMillis
			// log.Infof("有新消息 通知 PullProcessor 读取消息, Opaque: %d", command.Opaque)
			r.pool.Submit(func() {
				processor.PMap[code.PullMessage].Processor.ProcessRequest(command, pullRequest.ClientChannel)
			})
			continue
		}

		// suspend 过期
		timeOutMillis := pullRequest.SuspendTimestamp + pullRequest.TimeoutMillis
		if timeOutMillis < (time.Now().UnixNano() / 1e6) {
			var suspendTimeoutMillis int64 = 0
			command.ExtFields["SuspendTimeoutMillis"] = suspendTimeoutMillis
			log.Debugf("SuspendPullRequest 超时 PullProcessor 读取消息, Opaque: %d", command.Opaque)
			go processor.PMap[code.PullMessage].Processor.ProcessRequest(command, pullRequest.ClientChannel)
			continue
		}

		replayList.PushBack(pullRequest)
	}

	manyPullRequest.(*ManyPullRequest).pullRequestList.PushBackList(replayList)
}
