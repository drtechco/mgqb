package mgqb

import "fmt"

var defaultTraceLog = func(args ...interface{}) {
	fmt.Println("=================pipeline log start=================")
	fmt.Println(args...)
	fmt.Println("=================pipeline log end=================")
}

var defaultErrorLog = func(args ...interface{}) {
	fmt.Println(args...)
}
