package message

type Message struct {
	Topic         string
	Body          []byte
	Flag          int32
	transactionId string
	properties    map[string]string
}

type MessageExt struct {
	BrokerName                string
	QueueId                   int32
	StoreSize                 int32
	QueueOffset               int64
	SysFlag                   int32
	BornTimestamp             int64
	StoreTimestamp            int64
	BornHost                  string
	StoreHost                 string
	MsgId                     string
	CommitLogOffset           int64
	ReconsumeTimes            int32
	PreparedTransactionOffset int64
	BodyCrc                   int32
	Message
}

type CommandCustomHeader interface {
	checkFields()
}

type SendMessageRequestHeader struct {
	ProducerGroup string `json:"ProducerGroup"`
	Topic         string `json:"Topic"`
	QueueId       int32  `json:"QueueId"`
	BornTimestamp int64  `json:"BornTimestamp"`
}

func (r SendMessageRequestHeader) checkFields() {

}

type SendMessageResponseHeader struct {
	QueueOffset int64
	MsgId       string
	QueueId     int32
}

func (r SendMessageResponseHeader) checkFields() {

}

type PullMessageRequestHeader struct {
	ConsumerGroup string
	Topic         string
	QueueOffset   int64
	MaxMsgNums    int32
}
