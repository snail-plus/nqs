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
