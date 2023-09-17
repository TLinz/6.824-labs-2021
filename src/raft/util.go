package raft

import "log"

// Debugging
const Debug = false
const SDebug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Snlog(format string, a ...interface{}) (n int, err error) {
	if SDebug {
		log.Printf(format, a...)
	}
	return
}
