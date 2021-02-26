package util

import (
	"bytes"
	"encoding/binary"
	"net"
	"strconv"
	"strings"
)

func GetLocalAddress() string {
	var ip = "localhost"

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ip
	}

	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip = ipnet.IP.String()
			}
		}
	}

	return ip
}

func StrIpToInt(ipStr string) int {
	ipArr := strings.Split(ipStr, ".")
	var ipInt = 0
	var pos uint = 24
	for _, ipSeg := range ipArr {
		tempInt, _ := strconv.Atoi(ipSeg)
		tempInt = tempInt << pos
		ipInt = ipInt | tempInt
		pos -= 8
	}
	return ipInt
}

func StrIpToByte(ipStr string) []byte {
	return Int32ToBytes(StrIpToInt(ipStr))
}

func AddressToByte(address string) []byte {
	addressArr := strings.Split(address, ":")
	byteBuf := bytes.NewBuffer([]byte{})

	ipInt := int32(StrIpToInt(addressArr[0]))
	binary.Write(byteBuf, binary.BigEndian, ipInt)

	port, _ := strconv.Atoi(addressArr[1])
	binary.Write(byteBuf, binary.BigEndian, int32(port))
	return byteBuf.Bytes()
}

func ByteToAddress(b []byte) string {
	builder := strings.Builder{}
	for i := 0; i < 4; i++ {
		builder.WriteString(strconv.Itoa(BytesToInt8(b[i : i+1])))
		if i != 3 {
			builder.WriteString(".")
		}
	}

	portStr := strconv.Itoa(BytesToInt32(b[4:]))
	builder.WriteString(":")
	builder.WriteString(portStr)
	return builder.String()
}

func IpIntToStr(ipInt int) string {
	ipAdder := make([]string, 4)
	var len = len(ipAdder)
	buffer := bytes.NewBufferString("")
	for i := 0; i < len; i++ {
		tempInt := ipInt & 0xFF
		ipAdder[len-i-1] = strconv.Itoa(tempInt)
		ipInt = ipInt >> 8
	}

	for i := 0; i < len; i++ {
		buffer.WriteString(ipAdder[i])
		if i < len-1 {
			buffer.WriteString(".")
		}
	}
	return buffer.String()
}
