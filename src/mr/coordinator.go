package mr

import (
	"errors"
	"fmt"
	"log"
	"path"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorStatusEnum int
type TaskStatusEnum int

const LONG_TAIL_THRESHOLD = time.Second * 5
const COOR_WAIT_WORKER_EXIT_DURATION = time.Second * 10

const (
	CoorIdle CoordinatorStatusEnum = iota
	CoorMapping
	CoorReducing
	CoorFinish
)

const (
	TaskNotSend TaskStatusEnum = iota
	TaskDoing
	TaskDone
)

type TaskStatus struct {
	status         TaskStatusEnum
	beginTimestamp time.Time
}

type Coordinator struct {
	// Your definitions here.
	job                     *Job
	registerFinish          bool
	coordinatorStatus       CoordinatorStatusEnum
	jobLock                 sync.Mutex
	lastWorkerCallTimestamp time.Time
	teardownWorker          sync.Map
}

type Job struct {
	jobName             string
	fileName            []string
	nReduce             int
	mapTaskStatus       []TaskStatus
	mapOutputPathPrefix []string
	reduceTaskStatus    []TaskStatus
}

func (c *Coordinator) registerNewJob(filename []string, nReduce int) {
	newJob := Job{
		jobName:          c.getNewJobName(),
		fileName:         filename,
		nReduce:          nReduce,
		mapTaskStatus:    []TaskStatus{},
		reduceTaskStatus: []TaskStatus{},
	}
	newJob.mapTaskStatus = make([]TaskStatus, len(filename))
	newJob.reduceTaskStatus = make([]TaskStatus, nReduce)
	newJob.mapOutputPathPrefix = make([]string, len(filename))
	for i := range newJob.mapTaskStatus {
		newJob.mapTaskStatus[i] = TaskStatus{TaskNotSend, time.UnixMilli(0)}
	}
	for i := range newJob.reduceTaskStatus {
		newJob.reduceTaskStatus[i] = TaskStatus{TaskNotSend, time.UnixMilli(0)}
	}
	c.job = &newJob
	c.registerFinish = true
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) reportTaskFinish(jobType TaskType, index int) error {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	if jobType == Map {
		c.job.mapTaskStatus[index].status = TaskDone
	} else if jobType == Reduce {
		c.job.reduceTaskStatus[index].status = TaskDone
	} else {
		fmt.Println("unknown type when report, skip")
		return errors.New("unknown type")
	}
	return nil
}

func (c *Coordinator) getNewJobName() string {
	return "MRJob_" + strconv.FormatInt(time.Now().Unix(), 10)
}

// getIntermediatePath get the directory path for index-th map-worker
func (c *Coordinator) getIntermediatePath() string {
	cur, err := os.Getwd()
	if err != nil {
		fmt.Println("ERROR: get im path failed, use /tmp")
		return "/tmp"
		//return path.Join("/tmp", c.job.jobName, "intermediate", strconv.Itoa(index))
	}
	return cur
	//	return path.Join(cur, c.job.jobName, "intermediate", strconv.Itoa(index))
}

func (c *Coordinator) getOutputPath(index int) string {
	cur, err := os.Getwd()
	if err != nil {
		log.Fatal("ERROR: get current path failed")
	}
	return path.Join(cur, "mr-out-"+strconv.Itoa(index))
}

// need lock joblock
func (c *Coordinator) getRandomPendingTask(taskType TaskType) (error, HeartBeatTaskDesc) {
	var target []TaskStatus
	index := -1
	if taskType == Map {
		target = c.job.mapTaskStatus
	} else {
		target = c.job.reduceTaskStatus
	}
	for idx, item := range target {
		if item.status == TaskNotSend {
			index = idx
		}
	}
	if index == -1 {
		return errors.New("NO_PENDING_TASK"), HeartBeatTaskDesc{}
	}
	return c.getPendingTask(taskType, index)
}

// need lock joblock
func (c *Coordinator) getPendingTask(taskType TaskType, index int) (error, HeartBeatTaskDesc) {
	// todo: check index valid
	if taskType == Map {
		if c.job.mapTaskStatus[index].status == TaskDone {
			return errors.New("task has done"), HeartBeatTaskDesc{}
		} else {
			return nil, HeartBeatTaskDesc{
				TaskType:     taskType,
				NReduce:      c.job.nReduce,
				TaskIndex:    index,
				InputPath:    c.job.fileName[index],
				OutputPath:   c.getIntermediatePath(),
				TaskIsFinish: false,
			}
		}
	} else {
		if c.coordinatorStatus != CoorReducing {
			return errors.New("current coor status is not reducing"), HeartBeatTaskDesc{}
		}
		if c.job.reduceTaskStatus[index].status == TaskDone {
			return errors.New("task has done"), HeartBeatTaskDesc{}
		} else {
			return nil, HeartBeatTaskDesc{
				TaskType:     taskType,
				NReduce:      c.job.nReduce,
				TaskIndex:    index,
				InputPath:    c.getIntermediatePath(),
				OutputPath:   c.getOutputPath(index),
				TaskIsFinish: false,
			}
		}
	}
}

func (c *Coordinator) getMapTaskStatus() (int, int, int) {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	notsend, doing, done := 0, 0, 0
	for _, item := range c.job.mapTaskStatus {
		switch item.status {
		case TaskNotSend:
			notsend += 1
		case TaskDoing:
			doing += 1
		case TaskDone:
			done += 1
		}
	}
	return notsend, doing, done
}

func (c *Coordinator) getReduceTaskStatus() (int, int, int) {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()
	notsend, doing, done := 0, 0, 0
	for _, item := range c.job.reduceTaskStatus {
		switch item.status {
		case TaskNotSend:
			notsend += 1
		case TaskDoing:
			doing += 1
		case TaskDone:
			done += 1
		}
	}
	return notsend, doing, done
}

func (c *Coordinator) statusUpdaterLoop() {
	cnt := 10
	for {
		c.updateCoordinatorStatus()
		c.unregisterLongTailTask()
		time.Sleep(time.Second)
		cnt += 1
		if cnt%10 == 0 {
			var stateStr string
			switch c.coordinatorStatus {
			case CoorIdle:
				stateStr = "idle"
			case CoorMapping:
				stateStr = "mapping"
			case CoorReducing:
				stateStr = "reducing"
			case CoorFinish:
				stateStr = "finish"
			}
			mapNotSend, mapDoing, mapDone := c.getMapTaskStatus()
			reduceNotSend, reduceDoing, reduceDone := c.getReduceTaskStatus()
			fmt.Printf("Update Loop: current state = [%s], map:[%d/%d], reduce:[%d/%d]\n",
				stateStr, mapDone, mapNotSend+mapDoing+mapDone, reduceDone, reduceNotSend+reduceDoing+reduceDone)
		}
		if c.canExit() {
			os.Exit(0)
		}
	}
}

func (c *Coordinator) unregisterLongTailTask() {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()

	if c.coordinatorStatus == CoorMapping {
		for idx, item := range c.job.mapTaskStatus {
			if item.status == TaskDoing && time.Now().Sub(item.beginTimestamp) > LONG_TAIL_THRESHOLD {
				fmt.Printf("map task [%d] too slow, set status to not send\n", idx)
				c.job.mapTaskStatus[idx].status = TaskNotSend
			}
		}
	} else if c.coordinatorStatus == CoorReducing {
		for idx, item := range c.job.reduceTaskStatus {
			if item.status == TaskDoing && time.Now().Sub(item.beginTimestamp) > LONG_TAIL_THRESHOLD {
				fmt.Printf("reduce task [%d] too slow, set status to not send\n", idx)
				c.job.reduceTaskStatus[idx].status = TaskNotSend
			}
		}
	}
}

func (c *Coordinator) updateCoordinatorStatus() {
	c.jobLock.Lock()
	defer c.jobLock.Unlock()

	newState := CoorIdle
	allMapDone := true
	allReduceDone := true

	if c.registerFinish {
		newState = CoorMapping
	} else {
		goto setState
	}

	for _, item := range c.job.mapTaskStatus {
		if item.status != TaskDone {
			allMapDone = false
			break
		}
	}
	if allMapDone {
		newState = CoorReducing
	} else {
		goto setState
	}

	for _, item := range c.job.reduceTaskStatus {
		if item.status != TaskDone {
			allReduceDone = false
			break
		}
	}
	if allReduceDone {
		newState = CoorFinish
	}

	goto setState

setState:
	{
		c.coordinatorStatus = newState
		return
	}
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	reply.Pid = args.Pid
	reply.Teardown = false
	reply.IsNewTask = false
	reply.IsAck = false

	c.lastWorkerCallTimestamp = time.Now()
	c.teardownWorker.Store(args.Pid, 0)

	if c.coordinatorStatus == CoorIdle {
		reply.IsNewTask = false
		return nil
	}

	if c.coordinatorStatus == CoorFinish {
		reply.IsNewTask = false
		reply.Teardown = true
		fmt.Printf("Send a teardown reply to [%d]", args.Pid)
		c.teardownWorker.Store(args.Pid, 1)
		return nil
	}

	// pure heartbeat
	if args.CanAccessWork == false {
		reply.IsAck = true
		if args.TaskDesc.TaskIsFinish {
			// todo: check status is doing
			err := c.reportTaskFinish(args.TaskDesc.TaskType, args.TaskDesc.TaskIndex)
			c.updateCoordinatorStatus()
			if args.TaskDesc.TaskType == Map {
				fmt.Printf("Get a map task done rpc, index = [%d]\n", args.TaskDesc.TaskIndex)
			} else {
				fmt.Printf("Get a reduce task done rpc, index = [%d]\n", args.TaskDesc.TaskIndex)
			}
			if err != nil {
				return err
			}
		}
	} else {
		// schedule task
		var err error
		c.jobLock.Lock()
		defer c.jobLock.Unlock()
		if c.coordinatorStatus == CoorMapping {
			err, reply.TaskDesc = c.getRandomPendingTask(Map)
			if err == nil {
				reply.IsNewTask = true
				c.job.mapTaskStatus[reply.TaskDesc.TaskIndex].status = TaskDoing
				c.job.mapTaskStatus[reply.TaskDesc.TaskIndex].beginTimestamp = time.Now()
				fmt.Printf("Set a new map task to worker, index = [%d]\n", reply.TaskDesc.TaskIndex)
			} else if err.Error() != "NO_PENDING_TASK" {
				fmt.Printf("get an error when get random pending map task [%s]\n", err.Error())
			}
		} else if c.coordinatorStatus == CoorReducing {
			err, reply.TaskDesc = c.getRandomPendingTask(Reduce)
			if err == nil {
				reply.IsNewTask = true
				c.job.reduceTaskStatus[reply.TaskDesc.TaskIndex].status = TaskDoing
				c.job.reduceTaskStatus[reply.TaskDesc.TaskIndex].beginTimestamp = time.Now()
				fmt.Printf("Set a new reduce task to worker, index = [%d]\n", reply.TaskDesc.TaskIndex)
			} else if err.Error() != "NO_PENDING_TASK" {
				fmt.Printf("get an error when get random pending reduce task [%s]\n", err.Error())
			}
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) canExit() bool {
	c.updateCoordinatorStatus()

	existWorkerRunning := false
	c.teardownWorker.Range(func(key, value interface{}) bool {
		if value == 0 {
			existWorkerRunning = true
			return false
		}
		return true
	})
	if c.coordinatorStatus == CoorFinish &&
		(!existWorkerRunning || time.Now().Sub(c.lastWorkerCallTimestamp) >= COOR_WAIT_WORKER_EXIT_DURATION) {
		return true
	}

	return false
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.canExit()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{coordinatorStatus: CoorIdle, job: nil, lastWorkerCallTimestamp: time.UnixMilli(0), registerFinish: false}

	// Your code here.
	go c.statusUpdaterLoop()
	c.registerNewJob(files, nReduce)
	c.server()
	return &c
}
