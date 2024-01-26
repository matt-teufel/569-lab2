package shared

import (
	"math/rand"
	"time"
)

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

func (n Node) InitializeNeighbors(id int) [2]int {
	neighbor1 := RandInt()
	for neighbor1 == id {
		neighbor1 = RandInt()
	}
	neighbor2 := RandInt()
	for neighbor1 == neighbor2 || neighbor2 == id {
		neighbor2 = RandInt()
	}
	return [2]int{neighbor1, neighbor2}
}

func RandInt() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(MAX_NODES-1+1) + 1
}

/*---------------*/

// Membership struct represents participanting nodes
type Membership struct {
	Members map[int]Node
}

// Returns a new instance of a Membership (pointer).
func NewMembership() *Membership {
	return &Membership{
		Members: make(map[int]Node),
	}
}

// Adds a node to the membership list.
func (m *Membership) Add(payload Node, reply *Node) error {
	//TODO
	// fmt.Printf("Adding node %d\n", payload.ID);
	m.Members[payload.ID] = payload;
	*reply = m.Members[payload.ID];
	return nil;
}

// Updates a node in the membership list.
func (m *Membership) Update(payload Node, reply *Node) error {
	//TODO
	// fmt.Printf("Updating node %d\n", payload.ID);
	m.Members[payload.ID] = payload;
	*reply = m.Members[payload.ID];
	return nil;
}

// Returns a node with specific ID.
func (m *Membership) Get(payload int, reply *Node) error {
	//TODO
	*reply = m.Members[payload];
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
	req.Pending[payload.ID] = payload.Table;
	return nil;
}

// Listens to communication from neighboring nodes.
func (req *Requests) Listen(ID int, reply *Membership) error {
	neighborMembership, exists := req.Pending[ID];
	if (exists) { 
		// fmt.Printf("Found pending request for ID %d\n", ID);
		*reply = neighborMembership;
		delete(req.Pending, ID);
	// } else { 
	// 	fmt.Printf("No pending request for ID %d\n", ID);
	}
	return nil;
}

func CombineTables(table1 *Membership, table2 *Membership) *Membership {
	//TODO 
	// fmt.Println("Combining tables");
	var combinedTable Membership = *NewMembership();
	for key, value := range table2.Members { 
		combinedTable.Members[key] = value;
	}
	for key, value := range table1.Members {
		node2, keyPresent := table2.Members[key];
		if(!keyPresent || node2.Hbcounter < value.Hbcounter) {
			// fmt.Println("Updating node", key);
			combinedTable.Members[key] = value;
		}
	}
	return &combinedTable;
}

