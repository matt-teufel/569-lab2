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
	// if mem, exists := req.Pending[payload.ID]; exists {
	// 	newMem := CombineTables(&mem, &payload.Table);
	// 	req.Pending[payload.ID] = *newMem;
	// } else { 
		req.Pending[payload.ID] = payload.Table;
	// }
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
		// if (value.Alive) {
			// fmt.Println("first add")
			// printNode(value);
			combinedTable.Add(value, &reply);
		// }
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


type LogEntry struct { 
	term int 
	command string
}
type RaftNode struct {
	id              int
	currentTerm     int
	votedFor        int
	log             []LogEntry
	commitIndex     int
	appendIndex     int
	state           string //follower, candidate, leader 
}

func NewRaftNode(id int) *RaftNode {
	return &RaftNode{
		id:              id,
		currentTerm:     0,
		votedFor:        -1,
		log:             []LogEntry{{term: 0}},
		commitIndex:     0,
		appendIndex:     0,
		state:           "follower",
	}
}


type VoteRequest struct { 
	term int 
	id int
	lastLogIndex int
	lastLogTerm int
}

type VoteResponse struct { 
	term int 
	vote bool
	id int
}

// Requests struct represents pending message requests
type VoteRequests struct {
	Pending map[int]VoteRequest
	lock sync.Mutex
}

// Returns a new instance of a Membership (pointer).
func NewVoteRequests() *VoteRequests {                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
	//TODO
	return &VoteRequests{
		Pending: make(map[int]VoteRequest),
	}
}

func (req *VoteRequests) Add(payload VoteRequest, reply * bool) error {
	req.lock.Lock()
	defer req.lock.Unlock()
	req.Pending[payload.id] = payload;
	*reply = true;
	return nil;
}

func (req *VoteRequests) Listen(ID int, reply *VoteRequest) error {
	req.lock.Lock()
	defer req.lock.Unlock()
	if vote, exists := req.Pending[ID]; exists {
		*reply = vote;
	} else {
		reply = &VoteRequest{
			term: -1,
			id: -1,
			lastLogIndex: -1,
			lastLogTerm: -1,
		}
	}
	return nil;
}

type VoteResponses struct { 
	Pending map[int]VoteResponse;
	lock sync.Mutex;
}

func NewVoteResponses() * VoteResponses { 
	return &VoteResponses{
		Pending: make(map[int]VoteResponse),
	}
}

func (req *VoteResponses) Add(payload VoteResponse, reply * bool) error {
	req.lock.Lock()
	defer req.lock.Unlock()
	req.Pending[payload.id] = payload;
	*reply = true;
	return nil;
}

func (req *VoteResponses) Listen(ID int, reply *VoteResponse) error {
	req.lock.Lock()
	defer req.lock.Unlock()
	if vote, exists := req.Pending[ID]; exists {
		*reply = vote;
	} else {
		reply = &VoteResponse{
			term: -1,
			vote: false,
			id: -1,
		}
	}
	return nil;
} 
type AppendEntryRequest struct { 
	term int
	leaderId int
	clientId int
	prevLogIndex int
	prevLogTerm int
	entry LogEntry
	leaderCommit int
}

// Requests struct represents pending message requests
type AppendEntryRequests struct {
	Pending map[int]AppendEntryRequest
	lock sync.Mutex
}

// Returns a new instance of a Membership (pointer).
func NewAppendEntryRequests() *AppendEntryRequests {
	return &AppendEntryRequests{
		Pending: make(map[int]AppendEntryRequest),
	}
}

func (req *AppendEntryRequests) Add(payload AppendEntryRequest, reply * bool) error {
	req.lock.Lock()
	defer req.lock.Unlock()
	req.Pending[payload.clientId] = payload;
	*reply = true;
	return nil;
}

func (req *AppendEntryRequests) Listen(clientId int, reply *AppendEntryRequest) error {
	req.lock.Lock()
	defer req.lock.Unlock()
	if _, ok := req.Pending[clientId]; ok {
		*reply = req.Pending[clientId];
		delete(req.Pending, clientId);
		return nil;
	}
	*reply = AppendEntryRequest{
		term: -1,
		leaderId: -1,
		clientId: -1,
		prevLogIndex: -1,
		prevLogTerm: -1,
		entry: LogEntry{
			term: -1,
			command: "",
		},
		leaderCommit: -1,
	}
	return nil;
}

type AppendEntriesResponse struct {
	term int
	id int
	success bool
}

type AppendEntriesResponses struct {
	Pending map[int]AppendEntriesResponse
	lock sync.Mutex
}

func NewAppendEntriesResponses() *AppendEntriesResponses {
	return &AppendEntriesResponses{
		Pending: make(map[int]AppendEntriesResponse),
	}
}

func (resp *AppendEntriesResponses) Add(payload AppendEntriesResponse, reply *bool) error {
	resp.lock.Lock()
	defer resp.lock.Unlock()
	resp.Pending[payload.id] = payload;
	*reply = true;
	return nil;
}

func (resp *AppendEntriesResponses) Listen(id int, reply *AppendEntriesResponse) error {
	resp.lock.Lock()
	defer resp.lock.Unlock()
	if _, ok := resp.Pending[id]; ok {
		*reply = resp.Pending[id];
		delete(resp.Pending, id);
		return nil;
	}
	*reply = AppendEntriesResponse{
		term: -1,
		id: -1,
		success: false,
	}
	return nil;
}