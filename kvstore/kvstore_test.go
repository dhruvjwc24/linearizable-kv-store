package kvstore

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func setup() {
	// enable (limited) parallelism
	runtime.GOMAXPROCS(4)

	// clean up this user's directory
	ok := os.RemoveAll("/var/tmp/824-" + strconv.Itoa(os.Getuid()))
	if ok != nil {
		fmt.Printf("Warning: cannot remove Unix port directory\n")
	}
}

func check(t *testing.T, ck *KVClerk, key string, value string, expected bool) {
	v, present := ck.Get(key)
	if v != value || present != expected {
		t.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
	}
}

func checkWrite(t *testing.T, ck *KVClerk, op Op,
	key string, value string, result string, listener chan string) {
	doneChan := make(chan bool)

	go func() {
		writtenKey := <-listener
		writtenValue := <-listener

		if writtenKey != key || writtenValue != result {
			t.Fatalf("Bad disk write, expected [%s, %s] got [%s, %s]\n",
				key, value, writtenKey, writtenValue)
		}
		doneChan <- true
	}()

	if op == "Put" {
		ck.Put(key, value)
	} else {
		ck.Append(key, value)
	}

	select {
	case <-doneChan:
		return
	case <-time.After(time.Second):
		t.Fatalf("Timeout waiting for disk write")
	}
}

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	_ = os.Mkdir(s, 0777)
	s += "pb-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func randomChars(n int) string {
	letters := "abcdefghijklmnopqrstuvwxyz"
	value := ""
	for i := 0; i < n; i++ {
		value = value + string(letters[rand.Intn(26)])
	}
	return value
}

func TestEmptyGet(t *testing.T) {
	setup()

	sTag := "emptySrv"
	sPort := port(sTag, 1)
	server := StartServer(sPort, nil, 50)
	defer server.kill()
	ck := MakeClerk("emptyClient", sPort)
	defer ck.ShutDown()

	fmt.Printf("Test: get a non-existent key\n")

	check(t, ck, "noKey", "", false)

	fmt.Printf("passed\n")
}

func TestSimplePut(t *testing.T) {
	setup()

	sTag := "simplePut"
	sPort := port(sTag, 1)
	server := StartServer(sPort, nil, 50)
	defer server.kill()
	ck := MakeClerk("simpleClient", sPort)
	defer ck.ShutDown()

	fmt.Printf("Test: put a key, then get it\n")

	// Generate a random key (no hard-coded answers for credit!)

	key := "123"
	value := randomChars(3)
	ck.Put(key, value)
	check(t, ck, key, value, true)
	fmt.Printf("passed\n")
}

func TestAppend(t *testing.T) {
	setup()

	sTag := "append"
	sPort := port(sTag, 1)
	listenChan := make(chan string)
	server := StartServer(sPort, listenChan, 50)
	defer server.kill()
	ck := MakeClerk("appendClient", sPort)
	defer ck.ShutDown()
	fmt.Printf("Test: append to a key\n")

	key := "456"
	value := randomChars(3)

	// go func() { ck.Put(key, value) }()
	checkWrite(t, ck, "Put", key, value, value, listenChan)
	check(t, ck, key, value, true)

	value2 := randomChars(3)
	// go func() { ck.Append(key, value2) }()
	checkWrite(t, ck, "Append", key, value2, value+value2, listenChan)
	check(t, ck, key, value+value2, true)
	fmt.Printf("passed\n")
}

func TestConcurrentReadWrite(t *testing.T) {
	setup()

	sTag := "concRW"
	sPort := port(sTag, 1)
	timeChan := make(chan string)
	server := StartServer(sPort, timeChan, 0)
	defer server.kill()

	fmt.Printf("Reads and writes overlap\n")

	client1 := MakeClerk("client1", sPort)
	defer client1.ShutDown()
	client2 := MakeClerk("client2", sPort)
	defer client2.ShutDown()

	key := "shared"
	first := "first"
	second := "second"

	// Write to the key
	go func() { client1.Put(key, first) }()
	if <-timeChan != key || <-timeChan != first {
		t.Fatalf("Did not write first Put to disk correctly")
	}
	check(t, client1, key, first, true)

	// Overlap a read with that write
	go func() { client1.Put(key, second) }()
	<-timeChan
	check(t, client2, key, first, true)
	<-timeChan
	check(t, client2, key, second, true)
	fmt.Printf("passed\n")
}

func doDisjoint(t *testing.T, unreliable bool) {
	setup()

	sTag := "concDisj"
	if unreliable {
		sTag += "Un"
	}
	sPort := port(sTag, 1)
	server := StartServer(sPort, nil, 0)
	server.setUnreliable(unreliable)

	numClients := 10
	done := make(chan any)
	for i := 0; i < numClients; i++ {
		go func(i int) {
			ck := MakeClerk("client-"+strconv.Itoa(i), sPort)
			defer ck.ShutDown()
			key := "key-" + strconv.Itoa(i)
			theVal := ""
			for j := 0; j < 20; j++ {
				newValue := randomChars(3)
				ck.Append(key, newValue)
				theVal += newValue
				check(t, ck, key, theVal, true)
			}
			done <- nil
		}(i)
	}
	for i := 0; i < numClients; i++ {
		select {
		case <-done:
			continue
		case <-time.After(time.Second * 1):
			t.Fatalf("Client %v did not complete", i)
		}
	}
	server.kill()
}

func doOverlap(t *testing.T, unreliable bool) {
	setup()

	sTag := "concOverlap"
	if unreliable {
		sTag += "Un"
	}
	sPort := port(sTag, 1)
	server := StartServer(sPort, nil, 0)
	server.setUnreliable(unreliable)

	numClients := 4
	strLength := 5
	done := make(chan string)
	key := "shared"
	for i := 0; i < numClients; i++ {
		go func(i int) {
			ck := MakeClerk("client-"+strconv.Itoa(i), sPort)
			defer ck.ShutDown()
			myVal := ""
			for j := 0; j < strLength; j++ {
				thisVal := strconv.Itoa(i) + randomChars(2) + " "
				myVal += thisVal
				ck.Append(key, thisVal)
			}
			done <- myVal
		}(i)
	}
	var components [][]string
	var indices []int
	for i := 0; i < numClients; i++ {
		indices = append(indices, 0)
		select {
		case s := <-done:
			components = append(components, strings.Fields(s))
		case <-time.After(time.Second * 1):

			t.Fatalf("Client %v did not complete", i)
		}
	}
	ck := MakeClerk("client-reader", sPort)
	defer ck.ShutDown()
	composite, _ := ck.Get(key)
	terms := strings.Fields(composite)

	if len(terms) != numClients*strLength {
		t.Fatalf("Wrong number of components in jointly-appended string")
	}

Loop:
	for i := 0; i < len(terms); i++ {
		for j := 0; j < numClients; j++ {
			if indices[j] == strLength {
				continue // Skip this client' we've consumed its string.
			}
			if components[j][indices[j]] == terms[i] {
				indices[j]++
				continue Loop
			}
		}
		t.Fatalf("Term %v missing or out of order in components %v", terms[i], components)
	}
}

func TestConcurrentDisjoint(t *testing.T) {
	fmt.Printf("Test: concurrent operations on disjoint keys\n")
	doDisjoint(t, false)
	fmt.Printf("passed\n")
}

func TestConcurrentDisjointUnreliable(t *testing.T) {
	fmt.Printf("Test: concurrent operations on disjoint keys, unreliable network\n")
	doDisjoint(t, true)
	fmt.Printf("passed\n")
}

func TestConcurrentShared(t *testing.T) {
	fmt.Printf("Test: concurrent operations on shared keys\n")
	doOverlap(t, false)
	fmt.Printf("passed\n")
}

func TestConcurrentSharedUnreliable(t *testing.T) {
	fmt.Printf("Test: concurrent operations on shared keys, unreliable network\n")
	doOverlap(t, true)
	fmt.Printf("passed\n")
}
