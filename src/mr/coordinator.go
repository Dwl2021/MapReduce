package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Tasks               chan KeyValue_ofmap
	NReduce             int
	Notfinishedmap      int
	Notfinishedreduce   int
	Notfinishedconverge int
	Converge            int
	Converge_Num        chan int
	ReduckTask          chan KeyValue_ofreduce
	mu                  sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// init the coordinator
	c := Coordinator{}
	c.NReduce = nReduce
	c.Tasks = make(chan KeyValue_ofmap, 20)
	for index, filepath := range files {
		file, e := os.Open(filepath)
		if e != nil {
			log.Fatalf("Open error!")
		}
		content, e := io.ReadAll(file)
		if e != nil {
			log.Fatalf("Read error!")
		}
		tmp := KeyValue_ofmap{index, string(content)}
		c.Tasks <- tmp
		file.Close()
	}
	c.Notfinishedmap = len(c.Tasks)
	// reducetask := KeyValue_ofreduce{Key: 0, Value: make([]KeyValue, 1000)}
	// c.ReduckTask <- reducetask
	c.ReduckTask = make(chan KeyValue_ofreduce, 80)
	c.Notfinishedreduce = 80
	c.Converge = 0
	c.Converge_Num = make(chan int, 80)
	c.Notfinishedconverge = 0
	for i := 79; i > -1; i-- {
		c.Converge_Num <- i
	}
	c.server()
	return &c
}

func (c *Coordinator) Gettask(request Request, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Notfinishedmap != 0 && len(c.Tasks) != 0 {
		//first task -- mapping
		reply.Task = "map"
		reply.NReduce = c.NReduce
		// create temporary task to store the information in case the workers fail
		content := <-c.Tasks
		reply.Content = content
		// check the result
		timeout := 10.0 //timeout limit
		go check_finish_map(c, timeout, content)
		fmt.Printf("Content %d has been sent.\n", content.Key)
		// fmt.Println(reply.content.Key)
		return nil
	} else if c.Notfinishedmap != 0 && len(c.Tasks) == 0 {
		reply.Task = "wait"
		// fmt.Println(c.Notfinishedmap, len(c.Tasks))
		fmt.Println("Tasks are being mapping by other workers. Please rerequest...")
	} else if c.Notfinishedreduce != 0 && len(c.ReduckTask) != 0 && c.Converge == 0 {
		reply.Task = "reduce"
		reply.NReduce = c.NReduce
		content := <-c.ReduckTask
		reply.Content2 = content
		timeout := 10.0
		go check_finish_reduce(c, timeout, content)
	} else if c.Notfinishedreduce != 0 && len(c.ReduckTask) == 0 && c.Converge == 0 {
		reply.Task = "wait"
		fmt.Println("Tasks are being reduced by other workers. Please rerequest...")
	} else if c.Converge == 1 {
		reply.Task = "converge"
		if len(c.Converge_Num) <= 2 {
			if c.Notfinishedconverge == 0 {
				c.Notfinishedconverge++
				c.Converge = 2
				num1 := <-c.Converge_Num
				num2 := <-c.Converge_Num
				content := KeyValue_ofconverge{Key: -1, Num1: num1, Num2: num2}
				reply.Content3 = content
				go check_finish_converge(c, 10.0, content)
				if content.Key != -1 {
					// fmt.Printf("%d,%d have been send.\n", content.Num1, content.Num2)
				} else {
					fmt.Println("The last converge task have been send.")
				}
			} else {
				reply.Task = "wait"
				fmt.Println("Tasks are being converged by other workers. Please rerequest...")
			}
		} else {
			c.Notfinishedconverge++
			num1 := <-c.Converge_Num
			num2 := <-c.Converge_Num
			content := KeyValue_ofconverge{Key: num1, Num1: num1, Num2: num2}
			reply.Content3 = content
			go check_finish_converge(c, 10.0, content)
			fmt.Printf("%d,%d have been send.\n", content.Num1, content.Num2)
		}
	} else if c.Converge == 2 {
		reply.Task = "wait"
		fmt.Println("Tasks are being converged by other workers. Please rerequest...")
	} else {
		reply.Task = "null"
		fmt.Printf("All the tasks have been completed.\n")
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "localhost:12479")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname) //unix用于本地通信，tcp可以远程
	if e != nil {
		log.Fatal("Server listen to the ip or port error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ret := false
	if c.Notfinishedmap == 0 && c.Notfinishedreduce == 0 && c.Converge == 3 {
		ret = true
	}
	return ret
}

func split_into_value(filename string, c *Coordinator, index int) {
	data, err := readJSONFile(filename)
	if err != nil {
		// 处理错误
		return
	}
	pieces := 10
	length := len(data)
	chunkSize := length / pieces
	remaining := length % pieces
	startIndex := 0
	for i := 0; i < pieces; i++ {
		size := chunkSize
		if i < remaining {
			size++
		}
		chunk := data[startIndex : startIndex+size]
		tmp := KeyValue_ofreduce{Key: pieces*index + i, Value: chunk}
		c.mu.Lock()
		c.ReduckTask <- tmp
		c.mu.Unlock()
		startIndex += size
	}
}

func check_finish_converge(c *Coordinator, timeout float64, content KeyValue_ofconverge) {
	start := time.Now()
	num2 := content.Num2
	num1 := content.Num1
	check_file_exit := fmt.Sprintf("../main/reduce/mr-reduce-%d.json", num2)
	for {
		// 是否已经完成了任务
		if _, err := os.Stat(check_file_exit); err != nil { //file not exit
			c.mu.Lock()
			c.Converge_Num <- num1
			c.Notfinishedconverge--
			c.mu.Unlock()
			if content.Key == -1 {
				// all the task over
				c.mu.Lock()
				fmt.Printf("All the tasks have been over. Now release the resources.\n")
				c.Converge = 3
				c.mu.Unlock()
				All_tasks_over(num1)
			} else {
				fmt.Printf("File %d has completed converged.\n", content.Key)
			}
			break
		}
		// 检查是否超时
		elapsed := time.Since(start)
		if elapsed.Seconds() >= float64(timeout) {
			// c.mu.Lock() // 加锁
			c.Converge_Num <- num1
			c.Converge_Num <- num2
			// c.mu.Unlock() // 解锁
			fmt.Printf("File %d converged timed out and is being reissued.\n", content.Key)
			break
		}
		time.Sleep(time.Millisecond * 50) //500ms each check
	}
}

func check_finish_map(c *Coordinator, timeout float64, content KeyValue_ofmap) {
	start := time.Now()
	check_file_exit := fmt.Sprintf("../main/map/mr-map-%d.json", content.Key)
	for {
		// 是否已经完成了任务
		if _, err := os.Stat(check_file_exit); err == nil { //file exits
			c.mu.Lock()
			c.Notfinishedmap--
			c.mu.Unlock()
			fmt.Printf("File %d has completed mapping.\n", content.Key)
			split_into_value(check_file_exit, c, content.Key)
			break
		}
		// 检查是否超时
		elapsed := time.Since(start)
		if elapsed.Seconds() >= float64(timeout) {
			c.mu.Lock() // 加锁
			c.Tasks <- content
			c.mu.Unlock() // 解锁
			fmt.Printf("File %d mapping timed out and is being reissued.\n", content.Key)
			break
		}
		time.Sleep(time.Millisecond * 50) //500ms each check
	}
}

func check_finish_reduce(c *Coordinator, timeout float64, content KeyValue_ofreduce) {
	start := time.Now()
	check_file_exit := fmt.Sprintf("../main/reduce/mr-reduce-%d.json", content.Key)
	for {
		// 是否已经完成了任务
		if _, err := os.Stat(check_file_exit); err == nil { //file exits
			c.mu.Lock()
			c.Notfinishedreduce--
			fmt.Printf("File %d has completed reducing. Now not finished reduce leave %d\n. ", content.Key, c.Notfinishedreduce)
			c.mu.Unlock()
			if c.Notfinishedreduce == 0 {
				c.mu.Lock()
				fmt.Println("Now finish the reducing.")
				c.Converge = 1
				c.mu.Unlock()
			}
			break
		}
		// 检查是否超时
		elapsed := time.Since(start)
		if elapsed.Seconds() >= float64(timeout) {
			c.mu.Lock() // 加锁
			c.ReduckTask <- content
			fmt.Printf("File %d reducing timed out and is being reissued.\n", content.Key)
			c.mu.Unlock() // 解锁
			break
		}
		time.Sleep(time.Millisecond * 50) //500ms each check
	}
}

func readJSONFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	var data []KeyValue
	if err := decoder.Decode(&data); err != nil {
		return nil, err
	}
	return data, nil
}

func All_tasks_over(index int) {
	//改文件名
	thelastfile := fmt.Sprintf("../main/reduce/mr-reduce-%d.json", index)
	data, err := readJSONFile(thelastfile)
	if err != nil {
		log.Fatal("All tasks over error!")
	}
	oname := "../main/mr-out-0"
	ofile, _ := os.Create(oname)
	for i := 0; i < len(data); i++ {
		fmt.Fprintf(ofile, "%v %v\n", data[i].Key, data[i].Value)
	}
}
