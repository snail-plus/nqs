package message

type Message struct {
	Topic string
	Body []byte
}

type MessageExt struct {
	BrokerName string
	BornTimestamp int64
	StoreTimestamp int64
	QueueOffset int64
	QueueId int32
	CommitLogOffset int64
	Message
}

type CommandCustomHeader interface  {
	checkFields()
}

type SendMessageRequestHeader struct {
	ProducerGroup string `json:"ProducerGroup"`
	Topic string `json:"Topic"`
	QueueId int32 `json:"QueueId"`
	BornTimestamp int64 `json:"BornTimestamp"`
}

func (r SendMessageRequestHeader) checkFields() {

}

type SendMessageResponseHeader struct {
	QueueOffset int64
	MsgId string
	QueueId int32
}

func (r SendMessageResponseHeader) checkFields() {

}

type PullMessageRequestHeader struct {
	ConsumerGroup string
	Topic string
	QueueOffset int64
	MaxMsgNums int32
}