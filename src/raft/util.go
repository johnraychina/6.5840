package raft

import (
	"fmt"
	"log"
)

// Debugging
var Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func DTestPrintf(format string, a ...interface{}) {
	if Debug {
		log.Println("-----------------", fmt.Sprintf(format, a...), "-----------------")
	}
}
