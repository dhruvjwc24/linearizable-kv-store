package kvstore

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync/atomic"
	"syscall"
	"time"
)

type KVServer struct {
	l          net.Listener
	terminated chan any      // for testing; assigned only at init
	listener   chan string   // for testing; assigned only at init
	delay      time.Duration // for testing; assigned only at init
	unreliable int32         // for testing; requires synchronization
	me         string        // Assigned only at init
	impl       KVImpl        // Student responsibility
}

// tell the server to shut itself down.
func (kvs *KVServer) kill() {
	if !kvs.isDead() {
		fmt.Printf("Killing kvserver %v\n", kvs.me)
		close(kvs.terminated)
		err := kvs.l.Close()
		if err != nil {
			fmt.Printf("Error closing listener: %v\n", err)
		}
	} else {
		fmt.Printf("Error: kvserver %v killed twice.\n", kvs.me)
	}
}

// isDead: call this to find out if the server is dead.
func (kvs *KVServer) isDead() bool {
	select {
	case <-kvs.terminated:
		return true
	default:
		return false
	}
}

func (kvs *KVServer) setUnreliable(what bool) {
	if what {
		atomic.StoreInt32(&kvs.unreliable, 1)
	} else {
		atomic.StoreInt32(&kvs.unreliable, 0)
	}
}

func (kvs *KVServer) isUnreliable() bool {
	return atomic.LoadInt32(&kvs.unreliable) != 0
}

func StartServer(me string, listener chan string, delay int) *KVServer {
	kvs := new(KVServer)
	kvs.terminated = make(chan any)
	kvs.listener = listener
	kvs.delay = time.Duration(delay) * time.Millisecond
	kvs.me = me
	kvs.initKVImpl()

	rpcs := rpc.NewServer()
	ok := rpcs.Register(kvs)
	if ok != nil {
		fmt.Printf("Error registering RPCs: %v\n", kvs.me)
		os.Exit(1)
	}

	_ = os.Remove(kvs.me) // Ignore failure (typically b/c it already does not exist)
	l, e := net.Listen("unix", kvs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kvs.l = l

	go func() {
		for kvs.isDead() == false {
			conn, err := kvs.l.Accept()
			// We may have been killed while waiting for a new request
			if kvs.isDead() {
				if err == nil {
					_ = conn.Close() // Errors are immaterial for us
				}
				return
			}
			// If this accept resulted in an error, log it and try again
			if err != nil {
				fmt.Printf("KVServer(%v) accept: %v\n", me, err.Error())
				continue
			}
			// We are not dead, and have a valid connection
			// Did an unreliable network  cause the request to fail?
			if kvs.isUnreliable() && (rand.Int63()%1000) < 100 {
				// yes: discard it and do not process
				_ = conn.Close() // Errors are immaterial
				continue
			}
			// Will an unreliable network cause the response to fail?
			if kvs.isUnreliable() && (rand.Int63()%1000) < 200 {
				// yes: close file descriptor over which response will be sent
				c1 := conn.(*net.UnixConn)
				f, ok := c1.File()
				if ok != nil {
					fmt.Printf("KVServer(%v) file error: %v\n", me, ok.Error())
					os.Exit(1)
				}
				err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
				if err != nil {
					fmt.Printf("shutdown: %v\n", err)
				}
			}
			go rpcs.ServeConn(conn)
		}
	}()

	return kvs
}

// diskWrite
// Takes a Key/Value pair, and simulates writing it to disk.
// This routine doesn't really do anything other than pause to simulate
// blocking I/O. The testing infrastructure may optionally supply
// a listener channel; if non-nil, diskWrite sends key/value pairs
// for recording.
func (kvs *KVServer) diskWrite(key, value string) {
	if kvs.listener != nil {
		kvs.listener <- key
		kvs.listener <- value
	}
	time.Sleep(kvs.delay)
}
