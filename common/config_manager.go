package common

import (
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"nqs/util"
)

type IConfig interface {
	Encode() string
	Decode(string)
	ConfigFilePath() string
	Load() bool
	Persist()
}

type ConfigManager struct {
	Config IConfig
}

func (r *ConfigManager) Load() bool {
	fileName := r.Config.ConfigFilePath()
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

	r.Config.Decode(jsonString)

	return true
}

func (r *ConfigManager) Persist() {
	jsonStr := r.Config.Encode()
	fileName := r.Config.ConfigFilePath()
	if jsonStr == "" {
		return
	}

	err := util.StrToFile(jsonStr, fileName)
	if err != nil {
		log.Errorf("persist file: %s error: %s", fileName, err.Error())
	}

	log.Debugf("fileName: %s Persist ok", fileName)
}
