package channel

import (
	"errors"
	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"net"
	"nqs/remoting/protocol"
	"time"
)

const HeadLength = 4

var writePool, _ = ants.NewPool(4, ants.WithPreAlloc(true))

type Channel struct {
	Conn      net.Conn
	Encoder   protocol.Encoder
	Decoder   protocol.Decoder
	Closed    bool
	WriteChan chan *protocol.Command
}

func (r *Channel) IsOk() bool {
	return r.Conn != nil && !r.Closed
}

func (r *Channel) RemoteAddr() string {
	return r.Conn.RemoteAddr().String()
}

func (r *Channel) WriteCommand(command *protocol.Command) error {
	if r.Closed {
		return errors.New("连接已经关闭")
	}

	defer func() {
		if recover() != nil {

		}
	}()

	r.WriteChan <- command
	return nil
}

func (r *Channel) WriteToConn(command *protocol.Command) error {
	encode, err := r.Encoder.Encode(command)
	if err != nil {
		log.Errorf("Encode error: %s", err.Error())
		return err
	}

	r.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err2 := r.Conn.Write(encode)
	if err2 != nil {
		log.Errorf("Opaque: %d, write error: %s", command.Opaque, err2.Error())
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
