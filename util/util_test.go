package util

import (
	"fmt"
	"testing"
)

func TestA(t *testing.T) {
	bytes := Int64ToBytes(1)
	fmt.Println(len(bytes))
}


func TestB(t *testing.T) {
	bytes := BytesToInt64(Int64ToBytes(1))
	fmt.Println(bytes)
}

func TestC(t *testing.T) {
	bytes := BytesToInt32(Int32ToBytes(1))
	fmt.Println(bytes)
}