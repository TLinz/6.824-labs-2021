package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var mapf func(string, string) []KeyValue
var reducef func(string, []string) string

func handleMap(fn string, nr int, mtn int) {
	file, err := os.Open(fn)
	if err != nil {
		log.Fatalf("cannot open %v", fn)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fn)
	}
	file.Close()

	tempFiles := [](*os.File){}
	for i := 0; i < nr; i++ {
		tf, err := ioutil.TempFile("./", fmt.Sprintf("mr-%d-%d", mtn, i))
		if err != nil {
			log.Fatal("temp file create err")
		}
		tempFiles = append(tempFiles, tf)
	}

	kva := mapf(fn, string(content))
	for _, v := range kva {
		bucket := ihash(v.Key) % nr
		fmt.Fprintf(tempFiles[bucket], "%v %v\n", v.Key, v.Value)
	}

	for i := range tempFiles {
		os.Rename(tempFiles[i].Name(), fmt.Sprintf("mr-%d-%d", mtn, i))
	}

	finishTask(0, mtn)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func handleReduce(rtn int, nReduce int) {
	kv := []KeyValue{}
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, rtn)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("open intermediate file %s err", filename)
		}
		defer file.Close()

		br := bufio.NewReader(file)
		for {
			line, _, err := br.ReadLine()
			if err == io.EOF {
				break
			}
			item := strings.Split(string(line), " ")
			kv = append(kv, KeyValue{item[0], item[1]})
		}
	}
	sort.Sort(ByKey(kv))

	oname := fmt.Sprintf("mr-out-%d", rtn)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kv) {
		j := i + 1
		for j < len(kv) && kv[j].Key == kv[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kv[k].Value)
		}
		output := reducef(kv[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kv[i].Key, output)

		i = j
	}

	finishTask(1, rtn)
}

// main/mrworker.go calls this function.
func Worker(mapff func(string, string) []KeyValue,
	reduceff func(string, []string) string) {
	mapf = mapff
	reducef = reduceff

OUT:
	for {
		reply := askForTask()

		if !reply.IsAvailable && reply.Status != 2 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		switch reply.Status {
		case 0:
			handleMap(reply.MapFileName, reply.NReduce, reply.Mtn)
		case 1:
			handleReduce(reply.Rtn, reply.NMap)
		case 2:
			break OUT
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func askForTask() AssignTaskReply {
	args := ApplyTaskArgs{}
	reply := AssignTaskReply{}
	if ok := call("Coordinator.AssignTask", &args, &reply); !ok {
		log.Fatal("askForTask err")
	}
	return reply
}

func finishTask(tp int, tn int) {
	args := FinishTaskArgs{}
	args.Type = tp
	switch tp {
	case 0:
		args.Mtn = tn
	case 1:
		args.Rtn = tn
	}
	reply := FinishTaskReply{}
	if ok := call("Coordinator.FinishTask", &args, &reply); !ok {
		log.Fatal("askForTask err")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
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
