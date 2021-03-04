package common

import (
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"nqs/util"
)

type ConfigManager struct {
	Encode         func() string
	Decode         func(jsonString string)
	ConfigFilePath func() string
}

func (r *ConfigManager) Load() bool {
	fileName := r.ConfigFilePath()
	exists, err := util.PathExists(fileName)
	if err != nil {
		return false
	}

	if !exists {
		return true
	}

	fileData, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Errorf("read file error: %s", err.Error())
		return false
	}

	jsonString := string(fileData)
	if len(jsonString) == 0 {
		return true
	}

	r.Decode(jsonString)

	return true
}

func (r *ConfigManager) Persist() {
	jsonStr := r.Encode()
	fileName := r.ConfigFilePath()
	if jsonStr == "" {
		return
	}

	err := util.StrToFile(jsonStr, fileName)
	if err != nil {
		log.Errorf("persist file: %s error: %s", fileName, err.Error())
	}
}
