package kvstore

import (
	"fmt"
	"net/rpc"
)

// Error values
type Err string

const (
	OK            = "OK"       // Success
	ErrNoKey      = "ErrNoKey" // No key (Get only)
	ErrTerminated = "ErrTerminated"
)

// Operations
type Op string

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

// An Operation: Get, Put, or Append
//
// This can be sent from Client to Primary, or
// from Primary to Backup

// Operation Arguments
type OpArgs struct {
	Op     Op     // Operation being performed
	Key    string // Key being fetched/modified
	Value  string // Value to Put/Append (if modification)
	Client string // Identifier for client requesting this operation
	SeqNo  int    // Sequence # of this operation on this client
	Source string // Source of this call (Client ID or Primary ID)
}

// Operation Results
type OpReply struct {
	Err   Err    // One of the Err codes
	Value string // value of key (Get only)
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
