package main

import (
	"github.com/VerstraeteBert/plumber-sidecar/pkg/config"
	"github.com/VerstraeteBert/plumber-sidecar/pkg/runtime"
	"go.uber.org/zap"
)

func main() {
	// setup logging
	// TODO configure prod / dev logging
	logger, err := zap.NewDevelopment()
	if err != nil {
		zap.L().Panic(err.Error())
	}
	zap.ReplaceGlobals(logger)
	defer zap.L().Sync()

	// init config from env
	configObj, err := config.ReadConfig()
	if err != nil {
		zap.L().Panic(err.Error())
	}

	// bootstrap runtime
	err = runtime.InitRuntime(configObj)
	if err != nil {
		zap.L().Panic(err.Error())
	}
}
