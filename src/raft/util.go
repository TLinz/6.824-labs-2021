package raft

import "log"

// Debugging
const Debug = true

func DPrintln(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format+"\n", a...)
	}
	return
}
