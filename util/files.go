package util

import (
	"os"
	"strings"
)

type Files []os.FileInfo

func (r Files) Len() int {
	return len(r)
}

func (r Files) Less(i, j int) bool {
	return strings.Compare(r[i].Name(), r[j].Name()) < 0
}

func (r Files) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
