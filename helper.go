package main

import (
	"fmt"
)


func Format(format string, a ...interface{}) string{
	return fmt.Sprintf(format, a...)
}
