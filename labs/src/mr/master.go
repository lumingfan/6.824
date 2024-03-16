package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type WorkerType int 
const (
	ReduceType WorkerType = 0
	MapType WorkerType = 1
)

type TasksInfo struct {
	unfinished_tasks_ map[int]bool	
	inprocess_tasks_ map[int]bool
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
		t.inprocess_tasks_[k] = true
		delete(t.idle_tasks_, k)
		return k
	}

	panic("NoAvailableTasks must be checked before calling this function")
}

func (t *TasksInfo) finishTask(task_id int)  {
	delete(t.inprocess_tasks_, task_id)
	delete(t.unfinished_tasks_, task_id)
}

func (t *TasksInfo) existTaskInprocess(task_id int) bool {
	_, ok := t.inprocess_tasks_[task_id]
	return ok
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

func MakeTasksInfo(ntasks int) *TasksInfo {
	tasks := TasksInfo {
		unfinished_tasks_: map[int]bool{},
		inprocess_tasks_:  map[int]bool{},
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
	map_tasks_info_ *MapTasksInfo
		
	// N reduce tasks
	reduce_tasks_info_ *ReduceTasksInfo

	done_ bool;

	mutex_ sync.Mutex;
	reduce_Cond_ *sync.Cond;
}


// helper for RPC handler
func (m *Master) AllocateMapTask() int {
	return m.map_tasks_info_.tasks_.allocateTask()
}

func (m *Master) AllocateReduceTask() int {
	return m.reduce_tasks_info_.tasks_.allocateTask()
}

func (m *Master) GetMapFileName(map_task_id int) string {
	return m.map_tasks_info_.filenames_[map_task_id]	
}


// error when no available work
func (m *Master) ScheduleWork() (WorkerType, int, error) {
	// all map tasks has been allocated, allocate a reduce work
	m.mutex_.Lock()
	if (m.map_tasks_info_.tasks_.noAvailableTasks()) {
		// reduces can't start until the last map has finished
		for !m.map_tasks_info_.tasks_.tasksFinished() {
			// fmt.Println("reduce wait")
			m.reduce_Cond_.Wait()
			// fmt.Println("reduce restart")
		}

		if (m.reduce_tasks_info_.tasks_.noAvailableTasks()) {
			m.mutex_.Unlock()
			return ReduceType, 0, errors.New("no available reduce work")
		}
		// allocate a reduce task to worker
		task_id := m.AllocateReduceTask()
		m.mutex_.Unlock()
		return ReduceType, task_id, nil
	}

	// if (m.map_tasks_info_.tasks_.noAvailableTasks()) {
	// 	return MapType, 0, errors.New("no avaiable map work")
	// }
	// allocate a map task to worker
	task_id := m.AllocateMapTask()
	m.mutex_.Unlock()
	return MapType, task_id, nil
}

// RPC handler
func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *Reply) error {
	m.mutex_.Lock()
	done := m.done_
	reduce_tasks_num := m.reduce_tasks_info_.nreduce_
	map_tasks_num := len(m.map_tasks_info_.filenames_)
	m.mutex_.Unlock()

	reply.Done_ = done
	if done {
		return nil
	}
	
	worker_type, worker_id, err := m.ScheduleWork()

	if err != nil {
		reply.Done_ = true
		return nil
	}

	file_name := ""

	m.mutex_.Lock()
	if worker_type == MapType {
		file_name = m.GetMapFileName(worker_id)
	}
	m.mutex_.Unlock()

	reply.Type_ = worker_type
	reply.Task_id_ = worker_id
	reply.File_name_ = file_name
	reply.Reduce_tasks_num_ = reduce_tasks_num
	reply.Map_tasks_num_ = map_tasks_num
	return nil
}

func (m *Master) MapFinished(args *Args, reply *Reply) error {
	map_worker_id := args.Worker_id
	m.mutex_.Lock()	
	if m.map_tasks_info_.tasks_.existTaskInprocess(map_worker_id) {
		m.map_tasks_info_.tasks_.finishTask(map_worker_id)
	}
	if (m.map_tasks_info_.tasks_.tasksFinished()) {
		fmt.Println("map finished")
		m.reduce_Cond_.Broadcast()
	}
	m.mutex_.Unlock()
	return nil
}

func (m *Master) ReduceFinished(args *Args, reply *Reply) error {
	reduce_worker_id := args.Worker_id
	m.mutex_.Lock()
	if m.reduce_tasks_info_.tasks_.existTaskInprocess(reduce_worker_id) {
		m.reduce_tasks_info_.tasks_.finishTask(reduce_worker_id)
	}
	if m.reduce_tasks_info_.tasks_.tasksFinished() {
		fmt.Println("reduce finished")
		m.done_ = true
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

	m.reduce_Cond_ = sync.NewCond(&m.mutex_)

	// Your code here.
	m.server()
	return &m
}
