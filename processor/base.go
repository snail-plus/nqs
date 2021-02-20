package processor

import (
	"nqs/remoting/channel"
	"nqs/remoting/protocol"
)

var PMap = map[int32]Processor{}

var Encoder = &protocol.JsonEncoder{}
var Decoder = &protocol.JsonDecoder{}

type Processor interface {
	Reject() bool
	ProcessRequest(*protocol.Command, *channel.Channel)
}

