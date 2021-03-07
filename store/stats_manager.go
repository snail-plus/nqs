package store

import (
	"github.com/henrylee2cn/goutil/calendar/cron"
	log "github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
)

var statsMap = sync.Map{}

type CallSnapshot struct {
	timestamp int64
	times     int64
	value     int64
}

const (
	msgCost      = "msgCost"
	responseCost = "responseCost"
	processCost  = "processCost"
)

var corn = cron.New()

func init() {
	statsMap.Store(msgCost, &CallSnapshot{})
	statsMap.Store(responseCost, &CallSnapshot{})
	statsMap.Store(processCost, &CallSnapshot{})
}

func IncMsgCost(cost int64) {
	incCost(msgCost, cost)
}

func IncResponseCost(cost int64) {
	incCost(responseCost, cost)
}

func IncProcessCost(cost int64) {
	incCost(processCost, cost)
}

func incCost(key string, cost int64) {
	load, ok := statsMap.Load(key)
	if !ok {
		return
	}
	item := load.(*CallSnapshot)
	tmpTimes := atomic.AddInt64(&item.times, 1)
	item.times = tmpTimes

	tmpValue := atomic.AddInt64(&item.value, cost)
	item.value = tmpValue

	if tmpTimes%10000 == 0 {
		log.Infof("key: %s, cost: %d ms", key, tmpValue/tmpTimes/1000000)
	}
}
