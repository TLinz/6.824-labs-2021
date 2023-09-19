package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int
	CommandId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId  int
	CommandId int
	Key       string
}

type GetReply struct {
	Err   Err
	Value string
}
