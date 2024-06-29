package raft

import (
	"log"
)

// Debugging
var Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
