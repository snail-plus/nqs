package util

import (
	"os"
	"path/filepath"
	"strings"
)

func GetWordDir() string {
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	runPath := strings.Replace(dir, "\\", "/", -1)
	return runPath
}

func GetPid() int {
	return os.Getegid()
}
