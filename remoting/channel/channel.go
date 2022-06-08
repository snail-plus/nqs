package channel

import (
	"bufio"
	"errors"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"nqs/remoting/protocol"
	"nqs/util"
	"sync"
	"time"
)

const HeadLength = 4

type Channel struct {
	Conn      net.Conn
	bw        *bufio.Writer
	br        *bufio.Reader
	Closed    bool
	WriteChan chan *protocol.Command
	lock      sync.Mutex
}

func NewChannel(conn net.Conn, ch chan *protocol.Command) *Channel {
	return &Channel{
		Conn:      conn,
		WriteChan: ch,
		bw:        bufio.NewWriterSize(conn, 1024*4),
		br:        bufio.NewReaderSize(conn, 1024*4),
	}
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

	startTime := util.CurrentTimeMillis()
	err := protocol.EncodeWithFn(command, func(data []byte) error {
		s, ok := command.ExtFields["sendTime"]
		if ok {
			switch s.(type) {
			case int64:
				delay := util.CurrentTimeMillis() - s.(int64)
				if delay > 10 && time.Now().UnixMilli()%2 == 0 {
					log.Infof("写入之前延迟过高 cost: %d ms", delay)
				}
			default:

			}

		}

		err := r.Conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if err != nil {
			return err
		}
		_, err2 := r.bw.Write(data)

		if err2 != nil {
			log.Errorf("Opaque: %d, channel: %s, write error: %s",
				command.Opaque, r.Conn.LocalAddr().String(), err2.Error())
			return err2
		}

		err3 := r.bw.Flush()
		if err3 != nil {
			log.Errorf("Opaque: %d, flush error: %s", command.Opaque, err3.Error())
			return err2
		}

		delay := util.CurrentTimeMillis() - startTime

		if delay > 1 {
			log.Infof("write channel cost: %d ms too long", delay)
		}

		return nil
	})

	return err
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

func (r *Channel) ReadFully(len int) ([]byte, error) {
	var b = make([]byte, len, len)

	totalReadLen := 0
	for {
		readLen, err := io.ReadFull(r.br, b[totalReadLen:])
		if err != nil {
			return nil, err
		}

		totalReadLen += readLen
		if totalReadLen >= len {
			return b, nil
		}
	}

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
