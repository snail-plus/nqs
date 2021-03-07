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
)

var corn = cron.New()

func init() {
	statsMap.Store(msgCost, &CallSnapshot{})
	statsMap.Store(responseCost, &CallSnapshot{})
}

func IncMsgCost(cost int64) {
	incCost(msgCost, cost)
}

func IncResponseCost(cost int64) {
	incCost(responseCost, cost)
}

func incCost(key string, cost int64) {
	load, ok := statsMap.Load(key)
	if !ok {
		return
	}
	item := load.(*CallSnapshot)
	item.times = atomic.AddInt64(&item.times, 1)
	item.value = atomic.AddInt64(&item.value, cost)

	if item.times%100 == 0 {
		log.Infof("key: %s, cost: %d ms", key, item.value/item.times/1000000)
	}
}
