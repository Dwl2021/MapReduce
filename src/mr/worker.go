package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"sync"
	"time"
)

var map_path string = "../main/"

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

// main/mrworker.go calls this function.
var wg sync.WaitGroup

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			Worker_call(mapf, reducef)
		}()
	}
	wg.Wait()
	cmd := exec.Command("rm", "-r", "../main/map")
	err := cmd.Run()
	if err != nil {
		fmt.Println("Delete map folder error.")
	}
	cmd = exec.Command("rm", "-r", "../main/reduce")
	err = cmd.Run()
	if err != nil {
		fmt.Println("Delete reduce folder error.")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func Worker_call(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reply := Reply{}
		request := Request{"request"}
		ok := call("Coordinator.Gettask", request, &reply) //check whether the RPC is successful
		if ok {
			if reply.Task == "map" {
				fmt.Printf("File %d has been received.\n", reply.Content.Key)
				intermediate := []KeyValue{}

				kva := mapf("", reply.Content.Value)
				intermediate = append(intermediate, kva...)
				saveToJSONFile("map", reply.Content.Key, intermediate)
				fmt.Printf("File %d has been written to the foleder.\n", reply.Content.Key)
				continue
			} else if reply.Task == "reduce" {
				fmt.Printf("File %d mapped has been received.\n", reply.Content2.Key)
				intermediate := reply.Content2.Value
				var intermediate_sorted_reduced []KeyValue
				sort.Sort(ByKey(intermediate))
				in := 0
				for in < len(intermediate) {
					j := in + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[in].Key {
						j++
					}
					values := []string{}
					for k := in; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[in].Key, values)
					intermediate_sorted_reduced = append(intermediate_sorted_reduced,
						KeyValue{Key: intermediate[in].Key, Value: output})
					in = j
				}
				saveToJSONFile("reduce", reply.Content2.Key, intermediate_sorted_reduced)
				fmt.Printf("File %d reduced has been written to the foleder.\n", reply.Content2.Key)
				continue
			} else if reply.Task == "converge" {
				fmt.Printf("File %d converged has been received.\n", reply.Content3.Key)
				filename1 := fmt.Sprintf(map_path+"reduce/mr-reduce-%d.json", reply.Content3.Num1)
				filename2 := fmt.Sprintf(map_path+"reduce/mr-reduce-%d.json", reply.Content3.Num2)
				data1, err1 := readJSONFile(filename1)
				data2, err2 := readJSONFile(filename2)
				if err1 != nil || err2 != nil {
					// 处理错误
					log.Fatal(err1, err2)
					return
				}
				data := append(data1, data2...)

				var data_sorted_reduced []KeyValue
				sort.Sort(ByKey(data))
				in := 0
				for in < len(data) {
					j := in + 1
					for j < len(data) && data[j].Key == data[in].Key {
						j++
					}
					values := []string{}
					for k := in; k < j; k++ {
						values = append(values, data[k].Value)
					}
					// output := reducef(data[in].Key, values)
					sum := 0
					for _, str := range values {
						num, err := strconv.Atoi(str)
						if err != nil {
							fmt.Printf("Error: %v\n", err)
							return
						}
						sum += num
					}
					output := strconv.Itoa(sum)
					data_sorted_reduced = append(data_sorted_reduced,
						KeyValue{Key: data[in].Key, Value: output})
					in = j
				}
				saveToJSONFile("reduce", reply.Content3.Num1, data_sorted_reduced)
				delete_file := map_path + fmt.Sprintf("reduce/mr-reduce-%d.json", reply.Content3.Num2)
				err := os.Remove(delete_file)
				if err != nil {
					fmt.Println("Error while delete file.", err)
					return
				}
				fmt.Printf("Converge %d has been finished.\n", reply.Content3.Key)
				continue
			} else if reply.Task == "wait" {
				fmt.Printf("Now rerequest...\n")
				time.Sleep(time.Second * 2)
				continue
			} else {
				fmt.Println("All tasks have been completed. Releasing now...")
				wg.Done()
				return
			}
		} else {
			fmt.Printf("Call failed! Can't connect to the server!\n")
			return
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, request Request, reply *Reply) bool {
	c, err := rpc.DialHTTP("tcp", "localhost:12479")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("Error while dialing: ", err)
	}
	defer c.Close()

	err = c.Call(rpcname, request, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

func saveToJSONFile(task string, index int, data []KeyValue) error {
	filename := fmt.Sprintf("/mr-%s-%d_tmp.json", task, index)
	file, err := os.Create(map_path + task + filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(data); err != nil {
		return err
	}
	newName := fmt.Sprintf("/mr-%s-%d.json", task, index)
	err = os.Rename(map_path+task+filename, map_path+task+newName)
	if err != nil {
		// 处理错误
		log.Fatal("Error renaming file: ", err)
	}
	return nil
}
