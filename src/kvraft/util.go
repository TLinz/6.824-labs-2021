package kvraft

import "log"

// Debugging
const Dbug = false

func DPrintln(format string, a ...interface{}) (n int, err error) {
	if Dbug {
		log.Printf(format+"\n", a...)
	}
	return
}
