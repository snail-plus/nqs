package protocol

import (
	"fmt"
	"nqs/common/message"
	"sync/atomic"
)

var index int32 = 0

const RpcOneway = 1
const RpcType = 0
const ResponseType = 1

type Command struct {
	Body []byte `json:"-"`

	/**
	请求编号
	*/
	Opaque int32 `json:"Opaque"`

	/**
	0 请求 1 响应
	*/
	Flag int32 `json:"Flag"`
	/**
	请求业务码
	*/
	Code int32 `json:"Code"`

	ExtFields map[string]interface{} `json:"ExtFields"`

	/**
	每个业务头部不同
	这里的数据从 ExtFields 填充
	*/
	CustomHeader message.CommandCustomHeader `json:"-"`
}

func (r *Command) CreateResponseCommand() *Command {

	c := &Command{}
	if r.IsOnewayRPC() {
		c.MarkOnewayRPC()
	} else {
		c.MarkResponseType()
	}

	c.Code = r.Code
	c.Opaque = r.Opaque
	c.CustomHeader = r.CustomHeader
	c.ExtFields = r.ExtFields
	return c
}

func (r *Command) String() string {
	return fmt.Sprintf(
		"Code=%d, Flag=%d,ExtFields=%v, Body=%s",
		r.Code, r.Flag, r.ExtFields, string(r.Body))
}

func CreatesRequestCommand() *Command {
	c := &Command{}
	c.Opaque = atomic.AddInt32(&index, 1)
	c.ExtFields = map[string]interface{}{}
	c.Flag = 0
	return c
}

func (r *Command) MarkOnewayRPC() {
	bits := 1 << RpcOneway
	r.Flag |= int32(bits)
}

func (r *Command) IsOnewayRPC() bool {
	bits := int32(1 << RpcOneway)
	return (r.Flag & bits) == bits
}

func (r *Command) MarkResponseType() {
	r.Flag = r.Flag | ResponseType
}

func (r *Command) IsResponseType() bool {
	return r.Flag&(ResponseType) == ResponseType
}
