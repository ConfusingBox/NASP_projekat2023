package utils

import (
	"encoding/json"
	"io/ioutil"
)

type Configuration struct {
	DatabaseURL string `json:"database_url"`
	Port        int    `json:"port"`
	LogLevel    string `json:"log_level"`
}

func loadConfiguration(filename string) (*Configuration, error) {
	configFile, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var config Configuration
	if err := json.Unmarshal(configFile, &config); err != nil { //Unmarshal == decode. Vraca nil ako dodje do nekog errora pri decodeovanju
		return nil, err
	}
	return &config, nil
}
