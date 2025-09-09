package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "time"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


// Rpc Request for Task
func Requesttask () (string,int,int,int ) {

	// Request task 
	args := RequestTask{}
	
	args.TaskRequest =  true

	reply := Respond{}


	ok := call("Coordinator.RespondCall", &args, &reply)


	if ok {
		if reply.CurrentTask == 1 {
			return reply.Filename,reply.Filenum,reply.NumReduce,1
		} else  if reply.CurrentTask == 2 {
			return "Reduce",reply.No_file,reply.ReduceIndex,2
		} else {
			return "",0,0,3
		}
		
	} else {
		return "-1",0,0,-1
	}

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	 //CallExample()

	 for {
	
		filename,index,NReduce,task := Requesttask() 


		if filename == "-1" {
			break 
		}

		if filename == "" {
			time.Sleep(time.Second)
		}

		if task == 1 {

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)

		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}

		file.Close()

		intermediate := []KeyValue{}

		kva := mapf(filename, string(content))

		for _ , kv := range kva {
			intermediate = append(intermediate,kv)
		}

		sort.Sort(ByKey(intermediate))


		buckets := make([][]KeyValue, NReduce)

		for i := range buckets {
			buckets[i] = []KeyValue{}
		}

		for id := range intermediate {
			buckets[ihash(intermediate[id].Key)%NReduce] = append(buckets[ihash(intermediate[id].Key)%NReduce],intermediate[id])
		}

		for id := range buckets {
			oname := "mr-" + strconv.Itoa(index) + "-" +strconv.Itoa(id)
			//fmt.Println(oname)
			inter_file, err := os.Create(oname)

			if err != nil {
				log.Fatalf("Can't create file ")
			}
			enc := json.NewEncoder(inter_file)
			for _, kv := range buckets[id] {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("Erorr writing file to intermediate file system")
				}
			}
			inter_file.Close()
		}

		arg := FinishedMap{}

		arg.FinishedIndex = index 

		reply := FinishedReply{}


		call("Coordinator.RecivedMap", &arg, &reply)

		} else  if task == 2 {

			number_of_file := index 
			number_of_reduce := NReduce


			oname := "mr-out-" + strconv.Itoa(number_of_reduce)

			ofile, _ := os.Create(oname)


			fin := []KeyValue{}

			for id := range number_of_file {
				read_file_name := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(number_of_reduce) 
				file,err := os.Open(read_file_name)
				if err != nil {
					log.Fatalf("file doesn't exist  %v",  read_file_name )
				}
				dec := json.NewDecoder(file)
				for {
    			var kv KeyValue
   				 if err := dec.Decode(&kv); err != nil {
      					break
   					}

   					fin = append(fin, kv)
  				}

			}
			sort.Sort(ByKey(fin))
		
		i := 0

		for i < len(fin) {
			j := i + 1
			for j < len(fin) && fin[j].Key == fin[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, fin[k].Value)
			}
			output := reducef(fin[i].Key, values)

		// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", fin[i].Key, output)

				i = j
			}

		arg := FinishedReduce{}

		arg.Reduce_index  = number_of_reduce 

		reply := ReduceReply{}

		call("Coordinator.RecivedReduce", &arg, &reply)


		}
	}


}



//
// example function to show how to make an RPC call to the coordinator.
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
