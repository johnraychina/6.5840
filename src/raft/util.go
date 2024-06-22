package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		now := time.Now()
		a = append(a, now.UnixMilli())
		log.Printf(format+" %d ms", a...)
	}
}
