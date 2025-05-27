package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
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
type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := RequestTask{}

		reply := Reply{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			os.Exit(0)
		} else {
			if reply.TaskType == 0 {
				fileToread := reply.Filename
				file, err := os.Open(fileToread)
				if err != nil {
					log.Fatalf("Failed to read" + fileToread)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("Couldn't read all lines")
				}
				Map(mapf, fileToread, string(content), reply.NReduce, reply.TaskIdx)
				file.Close()
			} else if reply.TaskType == 1 {
				Reduce(reducef, reply.TaskIdx)
			} else if reply.TaskType == 2 {
				time.Sleep(time.Second)
				continue
			} else {
				os.Exit(0)
			}
		}
	}

}

func Reduce(reducef func(string, []string) string, reduceTaskIdx int) {
	fmt.Printf("entered reduce\n")
	args := TaskDone{}
	reply := Reply{}
	args.TaskType = 1
	args.TaskIdx = reduceTaskIdx
	var intermediate []KeyValue
	files, _ := os.ReadDir("./")
	for _, f := range files {
		if strings.HasSuffix(f.Name(), fmt.Sprintf("-%d", reduceTaskIdx)) {
			file, _ := os.Open(f.Name())
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			file.Close()
		}
	}
	sort.Sort(ByKey(intermediate))
	i := 0
	oname := "mr-out-" + strconv.Itoa(reduceTaskIdx)
	ofile, _ := os.Create(oname)
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ok := call("Coordinator.TaskDoneReceive", &args, &reply)
	if !ok {
		log.Fatalf("Couldnt send task done - reduce")
	}
}

func Map(mapf func(string, string) []KeyValue, filename string, content string, NReduce int, taskIdx int) {
	intermediateMap := make([][]KeyValue, NReduce)
	intermediate := []KeyValue{}
	args := TaskDone{}
	args.TaskIdx = taskIdx
	args.TaskType = 0
	kva := mapf(filename, content)
	intermediate = append(intermediate, kva...)
	for _, tmp := range intermediate {
		intermediateMap[ihash(tmp.Key)%NReduce] = append(intermediateMap[ihash(tmp.Key)%NReduce], tmp)
	}
	_ = Partition(intermediateMap, taskIdx)
	reply := Reply{}
	ok := call("Coordinator.TaskDoneReceive", &args, &reply)
	if !ok {
		log.Fatalf("Couldnt send taskdone - map")
	}
	//fmt.Printf("Made file %v\n", kva)
}

func Partition(intermediateMap [][]KeyValue, taskIdx int) []string {
	var intermediateFiles []string
	randomPrefix := rand.Int()

	for i, kva := range intermediateMap {
		tempname := fmt.Sprintf("temp_%d_%d_%d", randomPrefix, taskIdx, i)
		tempfile, _ := os.OpenFile(tempname, os.O_RDWR|os.O_CREATE, 0644)
		enc := json.NewEncoder(tempfile)
		for _, kv := range kva {
			if err := enc.Encode(&kv); err != nil {
				fmt.Sprintf("lala")
			}
		}
		newname := fmt.Sprintf("m-%d-%d", taskIdx, i)
		if err := os.Rename(tempname, newname); err != nil {
			fmt.Sprintf("failed to rename")
		}
	}
	return intermediateFiles
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
