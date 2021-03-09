package util

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
)

var noSuchFieldError = errors.New("No such field")

func SetField(obj interface{}, name string, value interface{}) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return noSuchFieldError
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("Cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)

	if structFieldType != val.Type() {
		hasError := true
		if val.Type().Name() == "float64" {
			tmpValue := value.(float64)
			switch structFieldType.Name() {
			case "int16":
				val = reflect.ValueOf(int16(tmpValue))
				hasError = false
			case "int8":
				val = reflect.ValueOf(int8(tmpValue))
				hasError = false
			case "int32":
				val = reflect.ValueOf(int32(tmpValue))
				hasError = false
			case "int64":
				val = reflect.ValueOf(int64(tmpValue))
				hasError = false
			case "int":
				val = reflect.ValueOf(int(tmpValue))
				hasError = false
			default:

			}
		}

		if hasError {
			return errors.New("Provided value type didn't match obj field type, name: " + name + ", structFieldType type: " + structFieldType.Name() + ", value type: " + val.Type().Name())
		}
	}

	structFieldValue.Set(val)
	return nil
}

func MapToStruct(m map[string]interface{}, obj interface{}) error {
	index := 0
	for k, v := range m {
		index++
		err := SetField(obj, k, v)
		if err == noSuchFieldError {
			continue
		}

		if err != nil && index == len(m) {
			return err
		}
	}
	return nil
}

func StructToMap(obj interface{}) map[string]interface{} {
	obj1 := reflect.TypeOf(obj)
	obj2 := reflect.ValueOf(obj)
	if obj1 == nil {
		return map[string]interface{}{}
	}

	if obj1.Kind() != reflect.Struct {
		return map[string]interface{}{}
	}

	var data = make(map[string]interface{})
	for i := 0; i < obj1.NumField(); i++ {
		data[obj1.Field(i).Name] = obj2.Field(i).Interface()
	}
	return data
}

func DeepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}
