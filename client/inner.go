package client

type InnerConsumer interface {
	PersistConsumerOffset() error
}
