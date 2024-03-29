package protocol

import (
	"bytes"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"nqs/remoting/buffer"
	"nqs/util"
)

type serializer interface {
	encodeHeader(*Command) ([]byte, error)
	decodeHeader([]byte) (*Command, error)
}

type JsonCodec struct {
}

type MqCodec struct {
}

var defaultSerializer = &JsonCodec{}

func (r *MqCodec) decodeHeader(data []byte) (*Command, error) {
	return nil, nil
}

func (r *MqCodec) encodeHeader(c *Command) ([]byte, error) {
	return nil, nil
}

/**
解码 这里 只是解出来通用头部 其他 不管
*/
func (r *JsonCodec) decodeHeader(data []byte) (*Command, error) {
	// 数据格式 长度[4字节] 头部长度[4字节] 头部数据  body
	c := &Command{}

	// log.Debug("headerData: " + string(headerData) + " , headerLength: " + strconv.Itoa(headerLength) + " bodyLength: " + strconv.Itoa(bodyLength))
	err := json.Unmarshal(data, c)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (r *JsonCodec) encodeHeader(c *Command) ([]byte, error) {
	makeCustomHeaderToNet(c)
	headerBytes, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return headerBytes, nil
}

var bufPool = buffer.NewDefaultBufPool()

func EncodeWithFn(c *Command, fn func(b []byte) error) error {
	buf := bufPool.GetBuffer()
	defer bufPool.PutBuffer(buf)

	encode, err := Encode(c, buf)
	if err != nil {
		return err
	}

	return fn(encode.Bytes())
}

func Encode(c *Command, buf *bytes.Buffer) (*bytes.Buffer, error) {
	startTime := util.CurrentTimeMillis()

	makeCustomHeaderToNet(c)
	headerBytes, err := defaultSerializer.encodeHeader(c)
	if err != nil {
		return nil, err
	}

	// log.Debugf("head json: %s", string(headerBytes))

	// 总长度 = 4 字节头部 + head 4 字节 + 头部数据 + body
	headLength := len(headerBytes)
	var bodyLength = 0
	if c.Body != nil {
		bodyLength = len(c.Body)
	}

	// 此长度不包含4字节头部
	totalLength := 4 + headLength + bodyLength
	//b := make([]byte, totalLength+4)

	// 写入总长度
	// copy(b[0:4], util.Int32ToBytes(totalLength))
	buf.Write(util.Int32ToBytes(totalLength))

	// log.Debugf("totalLength: %d", totalLength)

	// 写入head 长度
	// log.Debugf("headLength: %d", headLength)
	//copy(b[4:8], util.Int32ToBytes(headLength))
	buf.Write(util.Int32ToBytes(headLength))

	// 写入头部数据
	//copy(b[8:8+headLength], headerBytes)
	buf.Write(headerBytes)

	// 写入BODY 数据
	if c.Body != nil {
		//copy(b[8+headLength:], c.Body)
		buf.Write(c.Body)
		// log.Debugf("bodyLength: %d", len(c.Body))
	}

	delay := util.CurrentTimeMillis() - startTime
	if delay > 1 {
		log.Warnf("Encode cost: %d ms too long", delay)
	}

	return buf, nil
}

func Decode(data []byte) (*Command, error) {
	c := &Command{}
	length := len(data)
	headerLength := util.BytesToInt32(data[0:4])
	bodyLength := length - headerLength - 4

	headerData := data[4 : headerLength+4]

	c, err := defaultSerializer.decodeHeader(headerData)
	if err != nil {
		return nil, err
	}

	var bodyData []byte = nil
	if bodyLength > 0 {
		bodyData = data[headerLength+4:]
	}

	c.Body = bodyData
	return c, err
}

func makeCustomHeaderToNet(c *Command) {
	fields := c.ExtFields
	if fields == nil {
		fields = map[string]interface{}{}
	}

	if c.CustomHeader == nil {
		return
	}

	toMap := util.StructToMap(c.CustomHeader)
	// log.Debugf("toMap: %+v", toMap)
	for k, v := range toMap {
		fields[k] = v
	}

}
