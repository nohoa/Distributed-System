package lock

import (
	"6.5840/kvtest1"
)

//import "fmt"

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here 

	lockName string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	
	lk := &Lock{ck: ck,lockName : l}

	return lk
}

func (lk *Lock) Acquire() {

	// Your code here

	//for {
		lName := lk.lockName 
	
		_ = lk.ck.Acquire(lName) 

	// 	if status == "OK" {
	// 		break;
	// 	}
	// }

	return  
	
}

func (lk *Lock) Release() {
	// Your code here

	// for { 
	lName := lk.lockName 

	_ = lk.ck.Release(lName)

	// if status == "OK" {
	// 	break;
	// 	}

	// }

	//fmt.Println("Release success", lk.lockName )

}
