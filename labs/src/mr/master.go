package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type WorkerType int 
const (
	ReduceType WorkerType = 0
	MapType WorkerType = 1
)

type TasksInfo struct {
	unfinished_tasks_ map[int]bool	

	// channel is used by timer
	// if no reply in 10 secs, timer will fire, 
	// 	then give up this worker
	// stop timer when receive a reply in 10 secs
	inprocess_tasks_ map[int]chan bool
	idle_tasks_ map[int]bool
}

// no available tasks need to be allocated
func (t *TasksInfo) noAvailableTasks() bool {
	return len(t.idle_tasks_) == 0
}

// all tasks has been finished
func (t *TasksInfo) tasksFinished() bool {
	return len(t.unfinished_tasks_) == 0
}

// allocate a task 
func (t *TasksInfo) allocateTask() int {
	for k := range t.idle_tasks_ {
		t.inprocess_tasks_[k] = make(chan bool)
		delete(t.idle_tasks_, k)
		return k
	}

	panic("NoAvailableTasks must be checked before calling this function")
}

// finish a task
func (t *TasksInfo) finishTask(task_id int)  {
	delete(t.inprocess_tasks_, task_id)
	delete(t.unfinished_tasks_, task_id)
}

// change the task state from in-process to idle
// used by fail tolerance
func (t *TasksInfo) releaseTask(task_id int) {
	t.idle_tasks_[task_id] = true
	delete(t.inprocess_tasks_, task_id)
}

// Get crash channel, used by file tolerance
func (t *TasksInfo) GetWorkerCrashChannel(task_id int) (chan bool, bool) {
	ch, ok := t.inprocess_tasks_[task_id]
	return ch, ok
}

type MapTasksInfo struct {
	// M files
	filenames_ []string 
	tasks_ *TasksInfo
}

type ReduceTasksInfo struct {
	// N reduce
	nreduce_ int
	tasks_ *TasksInfo
}

// return a TasksInfo
func MakeTasksInfo(ntasks int) *TasksInfo {
	tasks := TasksInfo{
		unfinished_tasks_: map[int]bool{},
		inprocess_tasks_:  map[int]chan bool{},
		idle_tasks_:       map[int]bool{},
	}
	for idx := 0; idx < ntasks; idx++ {
		tasks.unfinished_tasks_[idx] = true
		tasks.idle_tasks_[idx] = true
	}		
	return &tasks
}

func MakeMapTasksInfo(filenames []string) *MapTasksInfo {
	map_tasks := MapTasksInfo{
		filenames_: filenames,
		tasks_:     MakeTasksInfo(len(filenames)),
	}	

	return &map_tasks
}

func MakeReduceTasksInfo(nreduce int) *ReduceTasksInfo {
	reduce_tasks := ReduceTasksInfo{
		nreduce_: nreduce,
		tasks_:   MakeTasksInfo(nreduce),
	}
	return &reduce_tasks
}

type Master struct {
	// Your definitions here.
	// M map tasks
	map_tasks_info_ *MapTasksInfo
		
	// N reduce tasks
	reduce_tasks_info_ *ReduceTasksInfo

	// is this job done
	done_ bool;

	// used to protect internal data 
	mutex_ sync.Mutex;

	// used to block/wake idle workers
	idle_workers_Cond_  *sync.Cond;
}


// helper for RPC handler

// wrapper
func (m *Master) AllocateMapTask() int {
	return m.map_tasks_info_.tasks_.allocateTask()
}
func (m *Master) AllocateReduceTask() int {
	return m.reduce_tasks_info_.tasks_.allocateTask()
}
func (m *Master) GetMapFileName(map_task_id int) string {
	return m.map_tasks_info_.filenames_[map_task_id]	
}

// release a inprocess task, then wake idle workers
// used by fail tolerance
func (m* Master) ReIssueTask(task_type WorkerType, task_id int) {
	m.mutex_.Lock()
	if task_type == MapType {
		m.map_tasks_info_.tasks_.releaseTask(task_id)
	} else {
		m.reduce_tasks_info_.tasks_.releaseTask(task_id)
	}
	m.idle_workers_Cond_.Broadcast()
	m.mutex_.Unlock()
}

// get a channel used by fail tolerance
func (m *Master) GetCrashChannel(task_type WorkerType, task_id int) (chan bool, bool){
	if task_type == MapType {
		return m.map_tasks_info_.tasks_.GetWorkerCrashChannel(task_id)
	} else {
		return m.reduce_tasks_info_.tasks_.GetWorkerCrashChannel(task_id)
	}
}

// start a timer, if worker doesn't reply in 10 secs
// re-issue the task to other workers
// fail tolerance
func (m* Master) TimerStart(task_type WorkerType, task_id int) {
	// get crash timer
	m.mutex_.Lock()
	crash_channel, ok := m.GetCrashChannel(task_type, task_id)	
	m.mutex_.Unlock()

	// this task is not in process
	if !ok {
		return 
	}

	// start timer
	timer := time.NewTimer(time.Second * 10)
	for {
		select {
		// task finished in 10 seconds
		case <- crash_channel:
			timer.Stop()
			return 
		
		// time-out
		case <- timer.C:
			m.ReIssueTask(task_type, task_id)
			return 
		}
	}
}


// wait in two cases
// 	1. this task is reduce-task and exists one map-task unfinished
//  2. all task has been allocated, it must be idle

// if waked workers find all tasks already be done, return true
func (m *Master) ScheduleWork() (WorkerType, int, bool) {
	// all map tasks has been allocated, allocate a reduce work
	if (m.map_tasks_info_.tasks_.noAvailableTasks()) {
		// reduces can't start until the last map has finished
		for !m.map_tasks_info_.tasks_.tasksFinished() {
			m.idle_workers_Cond_.Wait()
			if !m.map_tasks_info_.tasks_.noAvailableTasks() {
				return m.ScheduleWork()
			}
		}

		for m.reduce_tasks_info_.tasks_.noAvailableTasks() {
			m.idle_workers_Cond_.Wait()
			if m.done_ {
				return 0, 0, true 				
			}
		}
		// allocate a reduce task to worker
		task_id := m.AllocateReduceTask()
		return ReduceType, task_id, false
	}

	// allocate a map task to worker
	task_id := m.AllocateMapTask()
	return MapType, task_id, false
}

// RPC handler
// all workers request a task by this rpc
func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *Reply) error {
	m.mutex_.Lock()
	done := m.done_
	reduce_tasks_num := m.reduce_tasks_info_.nreduce_
	map_tasks_num := len(m.map_tasks_info_.filenames_)

	reply.Done_ = done
	if done {
		m.mutex_.Unlock()
		return nil
	}
	
	worker_type, worker_id, finished := m.ScheduleWork()

	if finished {
		m.mutex_.Unlock()
		reply.Done_ = true
		return nil
	}

	file_name := ""

	if worker_type == MapType {
		file_name = m.GetMapFileName(worker_id)
	}
	m.mutex_.Unlock()

	reply.Type_ = worker_type
	reply.Task_id_ = worker_id
	reply.File_name_ = file_name
	reply.Reduce_tasks_num_ = reduce_tasks_num
	reply.Map_tasks_num_ = map_tasks_num

	go m.TimerStart(worker_type, worker_id)
	return nil
}

// map workers call this rpc when they finish task
func (m *Master) MapFinished(args *Args, reply *Reply) error {
	map_worker_id := args.Worker_id
	
	m.mutex_.Lock()	
	// get crash timer channel
	ch, ok := m.map_tasks_info_.tasks_.GetWorkerCrashChannel(map_worker_id)
	m.mutex_.Unlock()
	// this task is not in process
	if !ok {
		return nil
	}
	// stop timer
	ch <- true 

	m.mutex_.Lock()
	// this task has been finished
	m.map_tasks_info_.tasks_.finishTask(map_worker_id)
	if (m.map_tasks_info_.tasks_.tasksFinished()) {
		// all map task finished, 
		// idle workers can apply for a reduce task
		m.idle_workers_Cond_.Broadcast()
	}
	m.mutex_.Unlock()
	return nil
}

func (m *Master) ReduceFinished(args *Args, reply *Reply) error {
	reduce_worker_id := args.Worker_id

	m.mutex_.Lock()
	ch, ok := m.reduce_tasks_info_.tasks_.GetWorkerCrashChannel(reduce_worker_id)
	m.mutex_.Unlock()
	if !ok {
		return nil
	}
	ch <- true 

	m.mutex_.Lock()

	m.reduce_tasks_info_.tasks_.finishTask(reduce_worker_id)
	// job finished, wake all waiting workers
	if m.reduce_tasks_info_.tasks_.tasksFinished() {
		m.done_ = true
		m.idle_workers_Cond_.Broadcast()
	}
	m.mutex_.Unlock()
	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// masrc/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return m.done_;
}

//
// create a Master.
// masrc/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		map_tasks_info_:    MakeMapTasksInfo(files),
		reduce_tasks_info_: MakeReduceTasksInfo(nReduce),
		done_:              false,
		mutex_:  sync.Mutex{},
	}

	m.idle_workers_Cond_ = sync.NewCond(&m.mutex_)

	// Your code here.
	m.server()
	return &m
}
