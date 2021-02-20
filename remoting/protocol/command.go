package protocol

import (
	"fmt"
	"nqs/common/message"
	"sync/atomic"
)

var index int32 = 0


type Command struct {

	Body []byte  `json:"-"`

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

func (r *Command) CreateResponseCommand()  *Command {
	c := &Command{}
	c.Code = r.Code
	c.Flag = 1
	c.Opaque = r.Opaque
	c.Body = r.Body
	c.CustomHeader = r.CustomHeader
	c.ExtFields = r.ExtFields
	return c
}

func (r *Command) String() string {
	return fmt.Sprintf(
		"Code=%d, Flag=%d,ExtFields=%v, Body=%s",
		        r.Code, r.Flag, r.ExtFields, string(r.Body))
}

func  CreatesRequestCommand()  *Command {
	c := &Command{}
	c.Opaque = atomic.AddInt32(&index, 1)
	c.ExtFields = map[string]interface{}{}
	c.Flag = 0
	return c
}