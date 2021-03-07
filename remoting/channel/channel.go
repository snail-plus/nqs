package channel

import (
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

	encode, err := r.Encoder.Encode(command)
	if err != nil {
		return err
	}

	_, err2 := r.Conn.Write(encode)
	if err2 != nil {
		return err2
	}

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
