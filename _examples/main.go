package main

import (
	"examples/simple"
	"fmt"
	"github.com/drtechco/mgqb"
)

func main() {
	mgqb.BSON_LOGGER = true
	mgqb.Trace_Log = func(args ...interface{}) {
		fmt.Println(args...)
	}
	mgqb.Error_Log = func(args ...interface{}) {
		fmt.Println(args...)
	}
	simple.AggregateMain()
	fmt.Println()
	simple.Findmain()
}
