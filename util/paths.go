package util

import (
	"os"
	"strings"
)

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func GetFileNameByFullPath(path string) string {
	split := strings.Split(path, "/")
	return split[len(split)-1]
}
