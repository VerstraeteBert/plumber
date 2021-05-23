package util

import (
	"fmt"
	"strings"
)

func ErrorsToString(errs []error) string {
	var b strings.Builder
	for i, e := range errs {
		if i < len(errs)-1 {
			fmt.Fprintf(&b, "%s\n", e.Error())
		} else {
			fmt.Fprintf(&b, "%s", e.Error())
		}
	}
	return b.String()
}
