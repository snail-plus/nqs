package util

import (
	"bytes"
	"encoding/binary"
)

func BytesToInt64(bys []byte) int64 {
	byteBuff := bytes.NewBuffer(bys)
	var data int64
	binary.Read(byteBuff, binary.BigEndian, &data)
	return data
}

func Int64ToBytes(n int64) []byte {
	data := n
	byteBuf := bytes.NewBuffer([]byte{})
	binary.Write(byteBuf, binary.BigEndian, data)
	return byteBuf.Bytes()
}

func BytesToInt32(bys []byte) int {
	byteBuff := bytes.NewBuffer(bys)
	var data int32
	binary.Read(byteBuff, binary.BigEndian, &data)
	return int(data)
}

func Int32ToBytes(n int) []byte {
	data := int32(n)
	byteBuf := bytes.NewBuffer([]byte{})
	binary.Write(byteBuf, binary.BigEndian, data)
	return byteBuf.Bytes()
}

func Int8ToBytes(n int) []byte {
	data := int8(n)
	byteBuf := bytes.NewBuffer([]byte{})
	binary.Write(byteBuf, binary.BigEndian, data)
	return byteBuf.Bytes()
}

func BytesToInt8(bys []byte) int {
	byteBuff := bytes.NewBuffer(bys)
	var data int8
	binary.Read(byteBuff, binary.BigEndian, &data)
	return int(data)
}

func Int16ToBytes(n int) []byte {
	data := int16(n)
	byteBuf := bytes.NewBuffer([]byte{})
	binary.Write(byteBuf, binary.BigEndian, data)
	return byteBuf.Bytes()
}
