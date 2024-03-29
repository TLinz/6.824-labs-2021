package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

var cmu sync.Mutex
var counter int

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu           sync.Mutex
	clerkId      int // unique identifier
	commandId    int // monotonically increasing command counteras
	lastLeaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.commandId = 0

	cmu.Lock()
	ck.clerkId = counter
	counter += 1
	cmu.Unlock()

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.commandId += 1

	for {
		for i := range ck.servers {
			srv := ck.servers[(i+ck.lastLeaderId)%len(ck.servers)]
			args := &GetArgs{ck.clerkId, ck.commandId, key}
			reply := &GetReply{}

			timeout := time.After(2000 * time.Millisecond)

			okCh := make(chan bool, 1)
			select {
			case okCh <- srv.Call("KVServer.Get", args, reply):
				if <-okCh {
					if reply.Err == OK {
						ck.lastLeaderId = i
						return reply.Value
					} else if reply.Err == ErrNoKey {
						ck.lastLeaderId = i
						return ""
					}
				}
			case <-timeout:
				continue
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.commandId += 1
	for {
		for i := range ck.servers {
			srv := ck.servers[(i+ck.lastLeaderId)%len(ck.servers)]
			args := &PutAppendArgs{key, value, op, ck.clerkId, ck.commandId}
			reply := &PutAppendReply{}

			timeout := time.After(2000 * time.Millisecond)

			okCh := make(chan bool, 1)
			select {
			case okCh <- srv.Call("KVServer.PutAppend", args, reply):
				if <-okCh {
					if reply.Err == OK {
						ck.lastLeaderId = i
						return
					}
				} else {
					continue
				}
			case <-timeout:
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
