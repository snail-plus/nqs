package client

type InnerConsumer interface {
	PersistConsumerOffset() error
}

type InnerProducer interface {
}
