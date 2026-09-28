package kvstore

import (
	"log"
	"strconv"
	"sync"
)

var (
	nameInitialized sync.Once
	term            chan any      // Termination channel for the serializers
	nameChan        <-chan string // Get a unique clerk name
)

func nameGenerator() {
	serial := 0

	c := make(chan string)
	nameChan = c

	for {
		serial = serial + 1
		next := "KVClient-" + strconv.Itoa(serial)
		select {
		case c <- next:
		case <-term:
			return
		}
	}
}

func nameInitialize() {
	nameInitialized.Do(func() {
		nameChan = make(chan string)
		go nameGenerator()
	})
}

type KVClerk struct {
	me      string
	server  string
	term    chan any
	seqChan chan int
}

func MakeClerk(me, server string) *KVClerk {
	nameInitialize()

	ck := new(KVClerk)

	if me == "" {
		ck.me = <-nameChan
	} else {
		ck.me = me
	}
	ck.server = server

	ck.term = make(chan any)
	ck.seqChan = make(chan int)

	go func() {
		seq := 1

		for {
			select {
			case <-ck.term:
				return
			case ck.seqChan <- seq:
				seq++
			}
		}
	}()

	return ck
}

func (ck *KVClerk) IsDone() bool {
	select {
	case <-ck.term:
		return true
	default:
		return false
	}
}

func (ck *KVClerk) SeqNum() int {
	if !ck.IsDone() {
		return <-ck.seqChan
	}
	panic("Asking for sequence number after done")
}

// Get a value for a key
func (ck *KVClerk) Get(key string) (string, bool) {

	var reply OpReply

	if ck.IsDone() {
		return "", false
	}

	log.Printf("%s: Getting value for key %s\n", ck.me, key)
	ck.doOperation(GET, key, "", &reply)

	if reply.Err == ErrNoKey {
		return "", false
	}
	return reply.Value, true
}

// Put updates the key's value.
func (ck *KVClerk) Put(key string, value string) bool {

	var reply OpReply

	if ck.IsDone() {
		return false
	}

	log.Printf("%s: Putting value %s for key %s\n", ck.me, value, key)
	ck.doOperation(PUT, key, value, &reply)
	return true
}

// Append a value to the key
func (ck *KVClerk) Append(key string, value string) bool {

	var reply OpReply

	if ck.IsDone() {
		return false
	}

	log.Printf("%s: Appending value %s to key %s\n", ck.me, value, key)
	ck.doOperation(APPEND, key, value, &reply)
	if reply.Err == ErrNoKey {
		return false
	}
	return true
}

// ShutDown the client
// Release any dynamic resources.
// May be called only once.
func (ck *KVClerk) ShutDown() {
	log.Printf("%s: Shutting down\n", ck.me)

	close(ck.term)
}
