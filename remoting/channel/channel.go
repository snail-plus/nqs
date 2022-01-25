package channel

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"nqs/remoting/protocol"
	"time"
)

const HeadLength = 4

type Channel struct {
	Conn      net.Conn
	Closed    bool
	WriteChan chan *protocol.Command
}

func (r *Channel) IsOk() bool {
	return r.Conn != nil && !r.Closed
}

func (r *Channel) Destroy() {
	r.Closed = true
	close(r.WriteChan)
	r.Conn.Close()
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

	if command == nil {
		return nil
	}

	r.WriteChan <- command
	return nil
}

func (r *Channel) WriteToConn(command *protocol.Command) error {
	if command.IsResponseType() && command.IsOnewayRPC() {
		return nil
	}

	encode, err := protocol.Encode(command)
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

func (r *Channel) IsClosed(err error) bool {
	if !r.Closed {
		return false
	}

	opErr, ok := err.(*net.OpError)
	if !ok {
		return false
	}

	return opErr.Err.Error() == "use of closed network connection"
}

func ReadFully(len int, conn net.Conn) ([]byte, error) {
	var b = make([]byte, len, len)

	_, err := io.ReadFull(conn, b)
	if err != nil {
		return nil, err
	}

	return b, nil
	/*
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
		}*/
}
