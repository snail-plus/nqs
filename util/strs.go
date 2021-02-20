package util

import "strings"

func LowerFirstWord(inputStr string) string {
	return strings.ToLower(inputStr[0:1]) + inputStr[1:]
}

func UpperFirstWord(inputStr string) string {
	return strings.ToUpper(inputStr[0:1]) + inputStr[1:]
}