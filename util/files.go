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

func StrToFile(str, fileName string) error {
	split := strings.Split(fileName, "/")
	filePath := strings.Join(split[0:len(split)-1], "/")
	err := os.MkdirAll(filePath, 777)
	if err != nil {
		return err
	}

	tmpFileName := fileName + ".tmp"
	tmpFile, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	_, err = tmpFile.WriteString(str)
	tmpFile.Close()

	if err != nil {
		return err
	}

	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}
