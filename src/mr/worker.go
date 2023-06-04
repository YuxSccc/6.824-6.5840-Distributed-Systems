package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

type MRWorker struct {
	task            HeartBeatTaskDesc
	mapf            func(string, string) []KeyValue
	reducef         func(string, []string) string
	taskIsAvailable bool
}

func (w *MRWorker) doMapTask() bool {
	filename := w.task.InputPath

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		return false
	}
	intermediate := w.mapf(filename, string(content))
	// todo: use temp file and mv
	kvList := make([][]KeyValue, w.task.NReduce)
	for _, item := range intermediate {
		idx := ihash(item.Key) % w.task.NReduce
		kvList[idx] = append(kvList[idx], item)
	}

	convertF := func(kv []KeyValue) string {
		kvContent := ""
		for _, item := range kv {
			kvContent += item.Key + " " + item.Value + "\n"
		}
		return kvContent
	}

	for i := 0; i < w.task.NReduce; i++ {
		outfile := path.Join(w.task.OutputPath, "mr-"+strconv.Itoa(w.task.TaskIndex)+"-"+strconv.Itoa(i))
		err := os.WriteFile(outfile, []byte(convertF(kvList[i])), 0644)
		if err != nil {
			return false
		}
	}
	return true
}

func (w *MRWorker) doReduceTask() bool {
	var intermediate []KeyValue

	folder, err := os.Open(w.task.InputPath)
	if err != nil {
		fmt.Printf("Error opening folder: %v\n", err)
		return false
	}
	defer folder.Close()

	fileNames, err := folder.Readdirnames(-1)
	if err != nil {
		fmt.Printf("Error reading folder contents: %v\n", err)
		return false
	}

	pattern := fmt.Sprintf(`^mr-(.+)-%d$`, w.task.TaskIndex)
	regex, err := regexp.Compile(pattern)

	readKVFromFile := func(filename string) []KeyValue {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Error opening file: %v", err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		var res []KeyValue
		for scanner.Scan() {
			line := scanner.Text()

			// Split the line by space
			parts := strings.Split(line, " ")
			if len(parts) != 2 {
				log.Printf("Invalid line format: %s\n", line)
				continue
			}
			res = append(res, KeyValue{Key: parts[0], Value: parts[1]})
		}

		if scanner.Err() != nil {
			log.Fatalf("Error scanning file: %v", scanner.Err())
		}
		return res
	}

	for _, fileName := range fileNames {
		if regex.MatchString(fileName) {
			intermediate = append(intermediate, readKVFromFile(path.Join(w.task.InputPath, fileName))...)
		}
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", w.task.TaskIndex)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return true
}

// makeDecision return work done
func (w *MRWorker) makeDecision() bool {
	if w.taskIsAvailable {
		// do map/reduce
		if !w.task.TaskIsFinish {
			var taskDone bool
			if w.task.TaskType == Map {
				taskDone = w.doMapTask()
			} else {
				taskDone = w.doReduceTask()
			}
			if taskDone {
				w.task.TaskIsFinish = true
			}
		}
		args := HeartBeatArgs{
			Pid:           os.Getpid(),
			CanAccessWork: false,
			TaskDesc:      w.task,
		}
		ok, resp := w.rpcToCoor(args)
		if ok && w.task.TaskIsFinish && resp.IsAck {
			fmt.Printf("reported to coor task[%d] is done\n", w.task.TaskIndex)
			w.taskIsAvailable = false
		}
		return true

	} else {
		// request for a task or exit
		args := HeartBeatArgs{
			Pid:           os.Getpid(),
			CanAccessWork: true,
		}
		ok, resp := w.rpcToCoor(args)
		if !ok {
			fmt.Printf("call rpc failed\n")
			return true
		}

		if resp.Teardown {
			fmt.Printf("get Teardown message from coor\n")
			return false
		}

		if resp.IsNewTask {
			if resp.TaskDesc.TaskType == Map {
				fmt.Printf("get new map task from coor, index = [%d]\n", resp.TaskDesc.TaskIndex)
			} else {
				fmt.Printf("get new reduce task from coor, index = [%d]\n", resp.TaskDesc.TaskIndex)
			}
			w.task = resp.TaskDesc
			w.taskIsAvailable = true
		} else {
			fmt.Printf("there is no more unsend task, sleep 5s\n")
			time.Sleep(time.Second * 5)
		}
		return true
	}
}

func (w *MRWorker) Run() {
	for {
		if !w.makeDecision() {
			fmt.Println("worker exit")
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (w *MRWorker) rpcToCoor(args HeartBeatArgs) (bool, HeartBeatReply) {
	reply := HeartBeatReply{}
	ok := call("Coordinator.HeartBeat", &args, &reply)
	return ok, reply
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	worker := MRWorker{
		task:            HeartBeatTaskDesc{},
		mapf:            mapf,
		reducef:         reducef,
		taskIsAvailable: false,
	}

	worker.Run()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
