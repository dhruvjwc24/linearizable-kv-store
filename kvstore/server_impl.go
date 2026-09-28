package kvstore

// additions to KVServer state.
type KVImpl struct {
	kvMap map[string]string
	opMap map[string]Pair

	readChan           chan ReadReq
	writeChan          chan WriteReq
	reconciliationChan chan ReconciliationReq
}

// define a pair strucut for opMap
type Pair struct {
	SeqNo       int
	cachedReply OpReply
}

type ReadReq struct {
	key   string
	reply chan OpReply
}

type WriteReq struct {
	opArgs *OpArgs
	reply  chan OpReply
}

type ReconciliationReq struct {
	key   string
	value string
	reply chan OpReply
}

func (kvs *KVServer) initKVImpl() {
	/*
		General Design Flow:
		We have two goroutines acting as state owners. One strictly touches
		the "disk" (if there were to exist one). The other map-owner
		explicitly owns the key value store (aka the in-memory cache).
		When a PUT/APPEND request comes in, it releases the request into
		writeChan, and the disk-owner has a writeChan listener waiting to
		acquire the any requests traveling over the unbuffered channel.
		In that goroutine, the sequence number of the request is checked to
		see if it is a retry request. If so, the cachedReply from the opMap
		is just sent to the client. If it is truly a new operation, the
		request is sent to the disk, which (for the purposes of this project
		is just a delay simulating the extra time disk access takes). Once
		that delay elapses, the new kv pair is sent to be reconciled through
		the reconciliationChan, which routes into the map-owner goroutine.
		On the other hand, for a GET request, that is mapped directly into
		the map-owner goroutine over the readChan channel.
	*/

	kvs.impl.kvMap = make(map[string]string)
	kvs.impl.opMap = make(map[string]Pair)

	kvs.impl.readChan = make(chan ReadReq)
	kvs.impl.writeChan = make(chan WriteReq)
	kvs.impl.reconciliationChan = make(chan ReconciliationReq)

	// define map-owner goroutine
	go func() {
		for {
			select {
			/*
				case where there is an incoming req from readChannel
				(i.e. a Get)
			*/
			case req := <-kvs.impl.readChan: // req is type ReadReq
				value, keyExists := kvs.impl.kvMap[req.key]
				if keyExists {
					req.reply <- OpReply{OK, value}
				} else {
					req.reply <- OpReply{ErrNoKey, value}
				}

			/*
				case where there is an incoming req from
				reconciliationChan (i.e. a write to disk has been
				performed and that disk state is being reconciled
				with in-memory cache, or kvMap)
			*/
			case req := <-kvs.impl.reconciliationChan: // req is type ReconciliationReq
				kvs.impl.kvMap[req.key] = req.value
				req.reply <- OpReply{OK, req.value}

			case <-kvs.terminated:
				return
			}
		}
	}()

	// define disk-owner goroutine
	go func() {
		for {
			select {
			case req := <-kvs.impl.writeChan: // req is type WriteReq
				// check if the request exists within opMap
				if kvs.impl.opMap[req.opArgs.Client].SeqNo == req.opArgs.SeqNo {
					// return reply
					req.reply <- kvs.impl.opMap[req.opArgs.Client].cachedReply
				} else {
					key := req.opArgs.Key
					value := req.opArgs.Value

					// check if PUT or APPEND operation
					if req.opArgs.Op == APPEND { // if APPEND
						/*
							using readChan so that we only have one read being processed at
							a time, if we just read directly then we lose lexical confinement
							because under that idiom, only the map-owner goroutine should own
							kvMap reads
						*/

						respChan := make(chan OpReply)
						kvs.impl.readChan <- ReadReq{key, respChan}
						oldValue := (<-respChan).Value
						value = oldValue + value
					}

					// do diskWrite (just a delay)
					kvs.diskWrite(key, value)

					// update opMap with new SeqNo and cachedReply
					mapEntry := kvs.impl.opMap[req.opArgs.Client]
					mapEntry.SeqNo = req.opArgs.SeqNo
					mapEntry.cachedReply = OpReply{OK, value}
					kvs.impl.opMap[req.opArgs.Client] = mapEntry

					// reconcile with kvMap
					done := make(chan OpReply)
					kvs.impl.reconciliationChan <- ReconciliationReq{key: key, value: value, reply: done}
					<-done

					req.reply <- mapEntry.cachedReply
				}

			case <-kvs.terminated:
				return
			}
		}
	}()
}

func (kvs *KVServer) Operation(args *OpArgs, reply *OpReply) error {
	respChan := make(chan OpReply)

	if args.Op == GET { // route GET through readChan
		kvs.impl.readChan <- ReadReq{args.Key, respChan} // put type ReadReq into this
	} else { // route PUTS/APPENDS through writeChan
		kvs.impl.writeChan <- WriteReq{args, respChan}
	}

	response := <-respChan
	*reply = response
	return nil
}
