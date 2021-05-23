package config

import (
	"encoding/json"
	"os"
)

type confInputRef struct {
	Topic         string   `json:"topic"`
	Brokers       []string `json:"brokers"`
	ConsumerGroup string   `json:"consumerGroup"`
	InitialOffset string   `json:"initialOffset"`
}

type confOutputRef struct {
	Topic   string   `json:"topic"`
	Brokers []string `json:"brokers"`
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

// FIXME
//func (c *Config) validate() error {
//	if c.Topic == "" {
//		return &InvalidConfigError{field: "topic"}
//	}
//
//	if c.ConsumerGroup == "" {
//		return &InvalidConfigError{field: "consumerGroup"}
//	}
//
//	if len(c.Brokers) == 0 {
//		return &InvalidConfigError{field: "consumerGroup"}
//	}
//
//	if len(c.OutputTopics) == 0 {
//		return &InvalidConfigError{field: "outputTopics"}
//	}
//
//	return nil
//}

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
