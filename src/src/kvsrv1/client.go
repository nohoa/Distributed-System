package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)


import "time"

type Clerk struct {
	clnt   *tester.Clnt
	server string
	id string 
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.

	ck.id = kvtest.RandValue(8)

	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	 getArgs := rpc.GetArgs{key} 

	 getReply := rpc.GetReply{}

	for {

	 status := ck.clnt.Call(ck.server,"KVServer.Get",&getArgs,&getReply) 

	 if status {
		break 
	 }

	}


	return getReply.Value, getReply.Version, getReply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	putArgs := rpc.PutArgs{key,value,version} 

	putReply := rpc.PutReply{}

	status  := ck.clnt.Call(ck.server,"KVServer.Put",&putArgs,&putReply) 
	
	if status {
		if putReply.Err != rpc.ErrVersion  {
			return putReply.Err
		}
		return putReply.Err
	}

	time.Sleep(100 * time.Millisecond)

	retryputArgs := rpc.PutArgs{key,value,version} 

	retryputReply := rpc.PutReply{}

	for {

	status := ck.clnt.Call(ck.server,"KVServer.Put",&retryputArgs,&retryputReply)	

	if status {
		break
	} 

	}

	if putReply.Err == "" {
		if retryputReply.Err == rpc.ErrVersion {
			retryputReply.Err = rpc.ErrMaybe
		}

	} else {
		retryputReply.Err = rpc.ErrVersion
	}

	return retryputReply.Err  

}
func (ck * Clerk) Acquire(lkname string) rpc.Err {

	acquireArgs := rpc.AcquireArgs{lkname,ck.id} 

	acquireReply := rpc.AcquireReply{}

	for {

	status := ck.clnt.Call(ck.server,"KVServer.Acquire",&acquireArgs,&acquireReply) 

	if status == true {	
		if acquireReply.Err == rpc.OK {
				break 
			}
		}

	}


	 return acquireReply.Err 

}

func (ck * Clerk) Release(lkname string) rpc.Err {

	releaseArgs := rpc.ReleaseArgs{lkname,ck.id} 

	releaseReply := rpc.ReleaseReply{}

	for {

	status := ck.clnt.Call(ck.server,"KVServer.Release",&releaseArgs,&releaseReply) 
	
	//fmt.Println(status)

	if status == true {	
		if releaseReply.Err == rpc.OK {
				break 
			}
		}
	}


	 return releaseReply.Err 

}


