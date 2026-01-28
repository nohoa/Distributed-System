package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)


const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValVer struct {
	Val string 
	Ver rpc.Tversion
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	storage map[string]ValVer 

	Locks map[string]string // unique lock acquired by lockid
	

}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.storage = make(map[string]ValVer) 

	kv.Locks = make(map[string]string)
	
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	currKey := args.Key 

	kv.mu.Lock() 

	val,ok := kv.storage[currKey] 

	kv.mu.Unlock() 

	if !ok {
		reply.Err = rpc.ErrNoKey
	}	else {
		reply.Value = val.Val
		reply.Version = val.Ver 
		reply.Err = rpc.OK
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {

	// Your code here.

	currKey := args.Key 

	currValue := args.Value 

	currVer := args.Version

	kv.mu.Lock() 

	defer kv.mu.Unlock() 


	val,ok := kv.storage[currKey] 


	//print(val,ok)
	
	if !ok {
		if currVer != 0 {
			reply.Err = rpc.ErrNoKey
			return 
		}
		//kv.mu.Lock() 
		kv.storage[currKey] = ValVer{currValue,1}
		reply.Err = rpc.OK 
		return 		
	} else { 
		if currVer == val.Ver {
			//kv.mu.Lock() 
			kv.storage[currKey] = ValVer{currValue,currVer+1}
			reply.Err = rpc.OK 
			return 
		} else {
			reply.Err = rpc.ErrVersion
			return 
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {

}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}


func (kv *KVServer) Acquire(args *rpc.AcquireArgs, reply *rpc.AcquireReply) {

	kv.mu.Lock()

	defer kv.mu.Unlock()
	

	lname := args.Lock_name 

	cId := args.ClientId
	
	value,ok := kv.Locks[lname]

	if !ok {
		kv.Locks[lname] = cId
		reply.Err = rpc.OK
	} else if cId == value {
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrAcquire
	}
}


func (kv *KVServer) Release(args *rpc.ReleaseArgs, reply *rpc.ReleaseReply) {

	kv.mu.Lock()

	defer kv.mu.Unlock()



	lname := args.Lock_name 

	cId := args.ClientId
	
	value,ok := kv.Locks[lname]


	if !ok {
		reply.Err = rpc.OK 
	} else {
			if value != cId {
				reply.Err = rpc.ErrRelease
			} else {
			delete(kv.Locks,lname)
			reply.Err = rpc.OK
			}
		}
}