package util

import (
	"time"
)

func GetUnixTime() int64 {
	return time.Now().Unix()
}

func TimeCost(start time.Time) time.Duration {
	return time.Since(start)
}

func CurrentTimeMillis() int64 {
	return time.Now().UnixNano() / 1e6
}
