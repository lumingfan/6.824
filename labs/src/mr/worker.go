package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
func (a ByKey) Len() int { return len(a) }
func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] } 
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func ReadFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

func CreateTempFiles(map_worker_id int, nreduce int) ([]string, []string, []*os.File){
	final_file_names := []string{}
	temp_files := []*os.File{}
	temp_file_names :=[]string{}

	for reduce_no := 0; reduce_no < nreduce; reduce_no++ {
		fn := "mr-temp-" + strconv.Itoa(map_worker_id) + "-" + strconv.Itoa(reduce_no)
		final_file_names = append(final_file_names, fn)
		
		temp_file, err := os.CreateTemp(".", fn)
		if err != nil {
			log.Fatalf("cannot create temp file %v", fn)
		}
		temp_files = append(temp_files, temp_file)
		temp_file_names = append(temp_file_names, temp_file.Name())
	}
	return final_file_names, temp_file_names,temp_files
}



func RenameTempFiles(filenames []string, temp_filenames []string, files []*os.File) {
	for _, file := range files {
		file.Close()
	}

	for idx, filename := range filenames {
		os.Rename(temp_filenames[idx], filename)
	}
}

func GetFiles(reduce_task_id int, intermediate_files_num int) []*os.File{
	filename_suffix := strconv.Itoa(reduce_task_id)
	files := []*os.File{}
	for idx := 0; idx < intermediate_files_num; idx++ {
		filename := "mr-temp-" + strconv.Itoa(idx)  + "-" + filename_suffix
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		files = append(files, file)
	}
	return files
}

func CloseFiles(files []*os.File) {
	for _, file := range files {
		file.Close()
	}
}

func CreateJsonEncoders(files []*os.File) []*json.Encoder {
	encs := []*json.Encoder{}	
	for _, file := range files {
		encs = append(encs, json.NewEncoder(file))
	}
	return encs
}

func CreateJsonDecoders(files []*os.File) []*json.Decoder {
	decs := []*json.Decoder{}
	for _, file := range files {
		decs = append(decs, json.NewDecoder(file))
	}
	return decs
} 

func MapRoutine(map_worker_id int, nreduce int, filename string, mapf func(string, string) []KeyValue ) {
	kva := mapf(filename, string(ReadFile(filename)))

	filenames, temp_filenames, temp_files := CreateTempFiles(map_worker_id, nreduce)
	encs := CreateJsonEncoders(temp_files)

	for _, kv := range kva {
		reduce_idx := ihash(kv.Key) % nreduce
		err := encs[reduce_idx].Encode(&kv)
		if err != nil {
			log.Fatal("cannot encode ", kv, "to json pair")
		}
	} 

	RenameTempFiles(filenames, temp_filenames, temp_files)
}

func ReduceRoutine(reduce_worker_id int, intermediate_files_num int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	
	files := GetFiles(reduce_worker_id, intermediate_files_num)
	decs := CreateJsonDecoders(files)

	for _, dec := range decs {
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(int(reduce_worker_id))
	WriteToFileWithReduceF(oname, kva, reducef)
}

func WriteToFileWithReduceF(oname string, kva []KeyValue, reducef func(string, []string) string) {
	ofile, err := os.CreateTemp(".", oname)
	if err != nil {
		log.Fatalf("cannot create temp file %v", oname)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	os.Rename(ofile.Name(), oname)
}


//
// masrc/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	reply := CallRegisterWorker()
	for HandleReply(reply, mapf, reducef) {
		reply = CallRegisterWorker()
	}
}

// process according to reply
// if reply.done_, no more tasks need to be execute, return
// if reply.info_.type_ is MapType, execute map routine
// else if reply.info_.type_ is ReduceType, execute reduce routine
func HandleReply(reply Reply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	done := reply.Done_
	if done {
		return false
	}

	if reply.Type_ == MapType {
		MapRoutine(reply.Task_id_, reply.Reduce_tasks_num_, reply.File_name_, mapf)
		CallMapFinished(reply.Task_id_)
	} else if reply.Type_ == ReduceType {
		ReduceRoutine(reply.Task_id_, reply.Map_tasks_num_, reducef)
		CallReduceFinished(reply.Task_id_)
	}
	return true
}

// the function is used to register a worker to master
func CallRegisterWorker() Reply {
	args := RegisterWorkerArgs{}			
	reply := Reply{}
	call("Master.RegisterWorker", &args, &reply)
	return reply
}

// the function is used to notice master map has been finished
func CallMapFinished(map_worker_id int) {
	args := Args{Worker_id: map_worker_id}	
	reply := Reply{}
	call("Master.MapFinished", &args, &reply)
}

// the function is used to notice master reduce has been finished
func CallReduceFinished(reduce_worker_id int) {
	args := Args{Worker_id: reduce_worker_id}
	reply := Reply{}
	call("Master.ReduceFinished", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
