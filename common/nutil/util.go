package nutil

import (
	"strconv"
	"strings"
)

func Offset2FileName(offset int64) string {
	fileNameLength := 20
	offsetStr := strconv.FormatInt(offset, 10)
	return strings.Repeat("0", fileNameLength - len(offsetStr)) + offsetStr
}