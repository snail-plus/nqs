package util

import (
	"net"
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
