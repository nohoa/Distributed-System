package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Coordinator struct {
	// Your definitions here.
	FileMap []string 
	Mapped_finished []int // 0 is not assigned , 1 is pending, 2 is finished
	FileReduce [] string 
	Reduced_finished []int
	No_reduce int 
	Finished_map int 
	Finished_all int 
	mu sync.Mutex
}



func (c* Coordinator) announce_Map (index int)  {
	time.Sleep(10*time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Mapped_finished[index] != 2 {
		c.Mapped_finished[index] = 0
	}

}


func (c* Coordinator) announce_Reduce (index int)  {
	time.Sleep(10*time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Reduced_finished[index] != 2 {
		c.Reduced_finished[index] = 0
	}
}
// Your code here -- RPC handlers for the worker to call.
func (c* Coordinator) RespondCall (req *RequestTask, res *Respond) error  {
	c.mu.Lock();
	defer c.mu.Unlock()
	index := -1
	for id := range c.FileMap {
		if c.Mapped_finished[id] == 0  {
			index = id 
			break 
		}
	}
	res.NumReduce = c.No_reduce
	//fmt.Printf("%v",index)
	if index != -1 {
		res.Filename = c.FileMap[index]
		//fmt.Println("%v",index)
		c.Mapped_finished[index] = 1
		res.Filenum = index 
		res.CurrentTask = 1 
		go c.announce_Map(index)

	} else if c.Finished_map ==  len(c.FileMap) {
		for id := range c.No_reduce {
			if c.Reduced_finished[id] == 0 {
				c.Reduced_finished[id] = 1;
				res.No_file = len(c.FileMap)
				res.ReduceIndex = id 
				res.CurrentTask = 2
				go c.announce_Reduce(id)
				break
			}
		}
	} else {
		res.CurrentTask = -1
	}
	return nil
}


func (c* Coordinator) RecivedReduce (req *FinishedReduce , res * ReduceReply) error {
	c.mu.Lock()
	c.Reduced_finished[req.Reduce_index] = 2
	c.Finished_all ++
	c.mu.Unlock()
	return nil 

}

func (c* Coordinator) RecivedMap (req *FinishedMap , res * FinishedReply) error {
 	c.mu.Lock()
	//fmt.Println("Finish index is %v",req.FinishedIndex)
	defer c.mu.Unlock()
	c.Mapped_finished[req.FinishedIndex] = 2 
	c.Finished_map ++
	return nil 
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	c.mu.Lock()

	if c.Finished_all == c.No_reduce  {
		ret = true ;
	}

	c.mu.Unlock()


	return ret
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.FileMap = files 

	c.No_reduce = nReduce

	c.Mapped_finished = make([]int, len(files))

	c.Reduced_finished = make([]int ,nReduce)

	c.server()
	return &c
}
