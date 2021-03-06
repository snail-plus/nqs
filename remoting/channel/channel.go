package channel

import (
	"github.com/panjf2000/ants/v2"
	"net"
	"nqs/remoting/protocol"
)

const HeadLength = 4

type Channel struct {
	Conn    net.Conn
	Encoder protocol.Encoder
	Decoder protocol.Decoder
	Closed  bool
}

func (r *Channel) IsOk() bool {
	return r.Conn != nil && !r.Closed
}

func (r *Channel) RemoteAddr() string {
	return r.Conn.RemoteAddr().String()
}

func (r *Channel) WriteCommand(command *protocol.Command) error {

	ants.Submit(func() {
		encode, err1 := r.Encoder.Encode(command)
		if err1 != nil {
			return
		}
		_, err2 := r.Conn.Write(encode)
		if err2 != nil {
			return
		}
	})
	return nil
}

func ReadFully(len int, conn net.Conn) ([]byte, error) {
	var b = make([]byte, len, len)

	totalCount := 0
	for {
		readLength, err := conn.Read(b)

		if err != nil {
			return nil, err
		}

		totalCount += readLength
		if totalCount >= len {
			return b, nil
		}
	}
}
