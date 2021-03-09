package util

import "strings"

func LowerFirstWord(inputStr string) string {
	return strings.ToLower(inputStr[0:1]) + inputStr[1:]
}

func UpperFirstWord(inputStr string) string {
	return strings.ToUpper(inputStr[0:1]) + inputStr[1:]
}

func HashString(s string) int {
	val := []byte(s)
	var h int32

	for idx := range val {
		h = 31*h + int32(val[idx])
	}

	return int(h)
}
