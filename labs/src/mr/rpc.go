package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RegisterWorkerArgs struct {
	
}

type Args struct {
	Worker_id int
}

type Reply struct {
	Done_ bool
	Type_ WorkerType 
	Task_id_ int
	
	// map worker info
	File_name_ string
	Reduce_tasks_num_ int

	// reduce worker info
	Map_tasks_num_ int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
