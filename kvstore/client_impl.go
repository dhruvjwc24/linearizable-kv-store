package kvstore

// Perform an operation
//
// The operation must keep trying until the key/value
// server replies, or the client has been shut down.
func (ck *KVClerk) doOperation(op Op, key string,
	value string, reply *OpReply) {

	// define OpArgs type to send to server call
	args := OpArgs{
		Op:     op,
		Key:    key,
		Value:  value,
		Client: ck.me,
		SeqNo:  ck.SeqNum(), // init at 1
	}

	// start unbounded operation call + infinite retry upon failure loop
	for !ck.IsDone() {
		success := call(ck.server, "KVServer.Operation", args, reply)
		if success {
			return
		} // if successful, no need for retry, just return
		/*
			retry is implicit because the for loop is unbounded, iterating
			until the call is successful. if it isn't, just reiterate and
			make the op call with the same sequence number
		*/
	}

}
