package client

import (
	"bytes"
	"nqs/util"
	"os"
	"strconv"
)

type ClientConfig struct {
   Ip string
   Pid string
}

func BuildClient() ClientConfig {
	c := ClientConfig{}
	c.Ip = util.GetLocalAddress()
	c.Pid = strconv.Itoa(os.Getpid())
	return c
}

func (r ClientConfig) BuildMQClientId() string {
	b := bytes.Buffer{}
	b.WriteString(r.Ip)
	b.WriteString("@")
	b.WriteString(r.Pid)
	return b.String()
}
