package shared

import (
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)

func init() { 
	gob.Register(Membership{})
	gob.Register(Requests{})
	gob.Register(Node{})
}

const (
	MAX_NODES = 8
)

// Node struct represents a computing node.
type Node struct {
	ID        int
	Hbcounter int
	Time      float64
	Alive     bool
}

// Generate random crash time from 10-60 seconds
func (n Node) CrashTime() int {
	rand.Seed(time.Now().UnixNano())
	max := 60
	min := 10
	return rand.Intn(max-min) + min
}

func (n Node) InitializeNeighbors(id int, maxNodes int) [2]int {
	neighbor1 := (id + 1) % (maxNodes + 1)
	neighbor2 := (id + 2) % (maxNodes + 1);
	return [2]int{neighbor1, neighbor2}
}
// func (n Node) InitializeNeighbors(id int) [2]int {
// 	neighbor1 := RandInt()
// 	for neighbor1 == id {
// 		neighbor1 = RandInt()
// 	}
// 	neighbor2 := RandInt()
// 	for neighbor1 == neighbor2 || neighbor2 == id {
// 		neighbor2 = RandInt()
// 	}
// 	return [2]int{neighbor1, neighbor2}
// }

func RandInt() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(MAX_NODES-1+1) + 1
}

// Membership struct represents participanting nodes
type Membership struct {
	Members map[int]Node
	lock sync.Mutex
}

// Returns a new instance of a Membership (pointer).
func NewMembership() *Membership {
	return &Membership{
		Members: make(map[int]Node),
	}
}

// Adds a node to the membership list.
func (m *Membership) Add(payload Node, reply *Node) error {
	m.lock.Lock()
	m.Members[payload.ID] = payload;
	*reply = payload;
	m.lock.Unlock();
	return nil;
}

// Updates a node in the membership list.
func (m *Membership) Update(payload Node, reply *Node) error {
	//TODO
	// fmt.Printf("Updating node %d\n", payload.ID);
	m.lock.Lock()
	m.Members[payload.ID] = payload;
	*reply = payload;
	m.lock.Unlock();
	return nil;
}

// Returns a node with specific ID.
func (m *Membership) Get(payload int, reply *Node) error {
	//TODO
	m.lock.Lock();
	*reply = m.Members[payload];
	m.lock.Unlock();
	return nil;
}

// Reads from server 
func (m *Membership) Read(payload int, reply *Membership) error {
	//TODO
	m.lock.Lock();
	*reply = *m;
	m.lock.Unlock();
	return nil;
}

/*---------------*/

// Request struct represents a new message request to a client
type Request struct {
	ID    int
	Table Membership
}

// Requests struct represents pending message requests
type Requests struct {
	Pending map[int]Membership
	lock sync.Mutex
}

// Returns a new instance of a Membership (pointer).
func NewRequests() *Requests {
	//TODO
	return &Requests{
		Pending: make(map[int]Membership),
	}
}

// Adds a new message request to the pending list
func (req *Requests) Add(payload Request, reply *bool) error {
	//TODO
	req.lock.Lock();
	req.Pending[payload.ID] = payload.Table;
	req.lock.Unlock()
	*reply = true;	
	return nil;
}

// Listens to communication from neighboring nodes.
func (req *Requests) Listen(ID int, reply *Membership) error {
	req.lock.Lock()	
	defer req.lock.Unlock()
	if mem, exists := req.Pending[ID]; exists { 
		*reply = mem;
	} else { 
		reply = NewMembership();
	}
	return nil;
}

func CombineTables(table1 *Membership, table2 *Membership) *Membership {
	//TODO 
	// fmt.Println("Combining tables");
	table1.lock.Lock()	
	table2.lock.Lock()	
	defer table1.lock.Unlock()
	defer table2.lock.Unlock()
	// fmt.Println("Combining tables");
	// printMembership(*table1);
	// printMembership(*table2);

	var combinedTable *Membership = NewMembership();
	var reply Node;
	for _, value := range table2.Members {
		if (value.Alive) {
			// fmt.Println("first add")
			// printNode(value);
			combinedTable.Add(value, &reply);
		}
	}
	for key, value := range table1.Members {
		if(value.Alive) { 
			node2, keyPresent := table2.Members[key];
			if(!keyPresent || node2.Hbcounter < value.Hbcounter) {
				// fmt.Printf("second add\n")
				// printNode(value);
				combinedTable.Add(value, &reply);
			}
		}
	}
	// fmt.Println("done combining tables")
	return combinedTable;
}

// func printMembership(m Membership) {
// 	for _, val := range m.Members {
// 		status := "is Alive"
// 		if !val.Alive {
// 			status = "is Dead"
// 		}
// 		fmt.Printf("Node %d has hb %d, time %.1f and %s\n", val.ID, val.Hbcounter, val.Time, status)
// 	}
// 	fmt.Println("")
// }

// func printNode(n Node) { 
// 	fmt.Printf("combine tables node:  %d has hb %d, time %.1f and %s\n", n.ID, n.Hbcounter, n.Time, n.Alive);
// }