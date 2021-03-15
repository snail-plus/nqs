package store

type MessageArrivingListener interface {
	Arriving(topic string, queueId int32, logicOffset int64)
}
