package config

import (
	"encoding/json"
	"os"
)

type confInputRef struct {
	Topic         string   `json:"topic"`
	Bootstrap     []string `json:"bootstrap"`
	ConsumerGroup string   `json:"consumerGroup"`
	InitialOffset string   `json:"initialOffset"`
}

type confOutputRef struct {
	Topic     string   `json:"topic"`
	Bootstrap []string `json:"bootstrap"`
}

type confProcessorDetails struct {
	Name    string `json:"name"`
	Project string `json:"project"`
}

type Config struct {
	InputRef         confInputRef         `json:"inputRef"`
	ConfOutputRefs   []confOutputRef      `json:"outputRefs"`
	ProcessorDetails confProcessorDetails `json:"processorDetails"`
}

// FIXME validation
func ReadConfig() (Config, error) {
	jsonConf := []byte(os.Getenv("PLUMBER_CONFIG"))
	confObj := Config{}
	err := json.Unmarshal(jsonConf, &confObj)
	if err != nil {
		return Config{}, err
	}
	//err = confObj.validate()
	return confObj, err
}
