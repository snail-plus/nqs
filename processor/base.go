package processor

import (
	"github.com/panjf2000/ants/v2"
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
)

var PMap = map[int32]Pair{}

type Pair struct {
	Pool      *ants.Pool
	Processor Processor
}

var Encoder = &protocol.JsonEncoder{}
var Decoder = &protocol.JsonDecoder{}

type Processor interface {
	Reject() bool
	ProcessRequest(*protocol.Command, *channel.Channel)
}
