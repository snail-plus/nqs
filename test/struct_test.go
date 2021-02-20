package test

import (
	"nqs/util"
	"testing"
)

type A interface {
     a()
}


type B struct {
   Age int64
}

func (r B) a() {

}

func testa(a interface{}) {
	/*v.FieldByName("Age").SetInt(32)*/
	util.MapToStruct(map[string]interface{}{"Age": 1}, &a)
}

func TestA(t *testing.T) {
	b := B{Age: 0}
	testa(b)
}
