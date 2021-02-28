package main

import (
	"container/list"
	"github.com/edsrzf/mmap-go"
	"nqs/common"
	"nqs/util"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var testData = []byte("0123456789ABCDEF")
var testPath = filepath.Join(os.TempDir(), "testdata")

func init() {
	f := openFile(os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	f.Write(testData)
	f.Close()
}

func openFile(flags int) *os.File {
	println(testPath)
	f, err := os.OpenFile(testPath, flags, 0644)
	if err != nil {
		newF, _ := os.Create(testPath)
		return newF
	}
	return f
}

func TestRead(t *testing.T) {
	f := openFile(os.O_RDWR)
	defer f.Close()
	mmap, err := mmap.MapRegion(f, 1024*1024*1024, mmap.RDWR, 0, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
		return
	}

	defer mmap.Unmap()
	/*if !bytes.Equal(testData, mmap) {
		t.Errorf("mmap != testData: %q, %q", mmap, testData)
	}*/

	mmap[9] = 'X'
	mmap.Flush()

	/*fileData, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	if !bytes.Equal(fileData, []byte("012345678XABCDEF")) {
		t.Errorf("file wasn't modified")
	}*/

	// leave things how we found them
	mmap[9] = '9'
	mmap.Flush()
}

func TestRead2(t *testing.T) {
	/*a := []byte("abcd")
	buffer := bytes.NewBuffer(a)
	buffer.Reset()
	buffer.Write([]byte("c"))

	println(a)
	println(buffer.String())*/
	files := list.New()
	files.PushBack("a")
	files.PushBack("b")

	for item := files.Front(); item != nil; item = item.Next() {
		println(item.Value.(string))
	}

}

type MyDaemonTask struct {
	common.DaemonTask
}

func TestNet(t *testing.T) {
	a := MyDaemonTask{}
	a.Name = "test"
	a.Run = func() {
		time.Sleep(1)
		println("aaa")
	}
	a.Start()

	time.Sleep(5 * time.Second)

	toByte := util.AddressToByte("127.0.0.1:9090")
	println(len(toByte))
	println(util.ByteToAddress(toByte))
}

type A struct {
	a int
}

func (a *A) age() {
	a.a = 34
}

func TestA(t *testing.T) {
	a := A{}
	a.age()
	println(a.a)
}
