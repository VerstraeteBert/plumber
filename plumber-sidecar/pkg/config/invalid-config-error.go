package config

import "fmt"

type InvalidConfigError struct {
	field string
}

func (e *InvalidConfigError) Error() string {
	return fmt.Sprintf("%s not present in sidecar json config", e.field)
}
