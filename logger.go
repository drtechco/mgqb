package mgqb

import "fmt"

var defaultTraceLog = func(args ...interface{}) {
	fmt.Println(args...)
}

var defaultErrorLog = func(args ...interface{}) {
	fmt.Println(args...)
}
