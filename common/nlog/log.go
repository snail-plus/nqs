package nlog

import (
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type LogFormatter struct{}

func (f *LogFormatter) Format(entry *log.Entry) ([]byte, error) {
	timestamp := time.Now().Local().Format("01-02-15:04:05.000")

	var file string
	var len int
	if entry.Caller != nil {
		file = filepath.Base(entry.Caller.File)
		len = entry.Caller.Line
	}
	//fmt.Println(entry.Data)
	msg := fmt.Sprintf("%s [%s:%d][GOID:%d][%s] %s\n", timestamp, file, len, getGID(), strings.ToUpper(entry.Level.String()), entry.Message)
	return []byte(msg), nil
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func GetLogger() *log.Logger {
	return log.StandardLogger()
}

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&LogFormatter{})
	log.SetReportCaller(true)
	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only nlog the warning severity or above.
	log.SetLevel(log.InfoLevel)

	log.Info("init log")
	log.Debug("debug log")
}

