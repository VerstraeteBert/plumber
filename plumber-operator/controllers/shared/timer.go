package shared

import (
	"fmt"
	"github.com/go-logr/logr"
	"time"
)

func Elapsed(logger logr.Logger, what string) func() {
	start := time.Now()
	return func() {
		logger.Info(fmt.Sprintf("%s took %v\n", what, time.Since(start)))
	}
}
