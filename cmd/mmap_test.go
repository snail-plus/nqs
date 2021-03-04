package main

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	"io/ioutil"
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
	f := openFile(testPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	f.Write(testData)
	f.Close()
}

func openFile(filePath string, flags int) *os.File {
	println(testPath)
	f, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		newF, _ := os.Create(testPath)
		return newF
	}
	return f
}

func TestRead4(t *testing.T) {
	f := openFile("C:\\Users\\wj\\AppData\\Local\\Temp\\store\\commitlog\\00000000000000000000", os.O_RDWR)
	defer f.Close()

	mMap, err := mmap.MapRegion(f, 1024*1024*1024, mmap.RDWR, 0, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
		return
	}

	/*	var b = 23
		copy(mMap[0:4], util.Int32ToBytes(b))
		println("写入数据")
	*/
	var a int32
	buffer := bytes.NewBuffer(mMap[0:4])
	binary.Read(buffer, binary.BigEndian, &a)
	println(a)

	mMap.Flush()
	mMap.Unmap()
}

func TestRead(t *testing.T) {
	f := openFile(testPath, os.O_RDWR)
	defer f.Close()
	mmap, err := mmap.MapRegion(f, 1024, mmap.RDWR, 0, 0)
	if err != nil {
		t.Errorf("error mapping: %s", err)
		return
	}

	defer mmap.Unmap()
	/*if !bytes.Equal(testData, mmap) {
		t.Errorf("mmap != testData: %q, %q", mmap, testData)
	}*/

	// mmap[9] = 'X'
	copy(mmap[0:1], "X")
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

	buffer := bytes.NewBuffer(make([]byte, 20))
	var data byte = 2

	buffer.Reset()
	binary.Write(buffer, binary.BigEndian, data)
	j := buffer.Bytes()
	println(j[0])

}

func TestB(t *testing.T) {

	dirs, err := ioutil.ReadDir("C:\\Users\\wj\\AppData\\Local\\Temp\\store\\consumequeue")
	if err != nil {
		return
	}

	for _, item := range dirs {
		println(item.Name())
		println(item)
	}

	xx := []byte{1, 2, 3}
	buffer := bytes.NewBuffer(xx)

	for buffer.Len() > 0 {
		var d int8
		binary.Read(buffer, binary.BigEndian, &d)
		println(d)
	}

}

func TestABB(t *testing.T) {
	err := util.StrToFile("你好", "C:\\Users\\wj\\Desktop\\abg.txt")
	if err != nil {
		println(err.Error())
	}

}
