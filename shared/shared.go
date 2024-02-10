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

func (n Node) InitializeNeighbors(ID int, maxNodes int) [2]int {
	neighbor1 := (ID % (maxNodes-1))  + 1
	neighbor2 := (ID  % (maxNodes -1)) + 2;
	return [2]int{neighbor1, neighbor2}
}
// func (n Node) InitializeNeighbors(ID int) [2]int {
// 	neighbor1 := RandInt()
// 	for neighbor1 == ID {
// 		neighbor1 = RandInt()
// 	}
// 	neighbor2 := RandInt()
// 	for neighbor1 == neighbor2 || neighbor2 == ID {
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
	Term int 
	Command string
}
type RaftNode struct {
	ID              int
	CurrentTerm     int
	VotedFor        int
	Log             []LogEntry
	CommitIndex     int
	AppendIndex     int
	State           string //follower, candIDate, leader 
}

func NewRaftNode(ID int) *RaftNode {
	return &RaftNode{
		ID:              ID,
		CurrentTerm:     0,
		VotedFor:        -1,
		Log:             []LogEntry{{Term: 0}},
		CommitIndex:     0,
		AppendIndex:     0,
		State:           "follower",
	}
}


type VoteRequest struct { 
	Term int 
	LeaderId int
	ClientId int
	LastLogIndex int
	LastLogTerm int
}

type VoteResponse struct { 
	Term int 
	Vote bool
	ID int
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
	// fmt.Println("Adding Vote request");
	req.lock.Lock()
	defer req.lock.Unlock()
	req.Pending[payload.ClientId] = payload;
	*reply = true;
	return nil;
}

func (req *VoteRequests) Listen(ID int, reply *VoteRequest) error {
	// fmt.Println("Listening for Vote request");
	req.lock.Lock()
	defer req.lock.Unlock()
	if Vote, exists := req.Pending[ID]; exists {
		// fmt.Printf("Vote request found: %d %d %d %d %d\n", Vote.LeaderId,Vote.ClientId, Vote.Term, Vote.LastLogIndex, Vote.LastLogTerm);
		*reply = Vote;
	} else {
		// fmt.Println("Vote request not found");
		*reply = VoteRequest{
			Term: -1,
			ClientId: -1,
			LeaderId: -1,
			LastLogIndex: -1,
			LastLogTerm: -1,
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
	// fmt.Println("adding Vote response");
	req.lock.Lock()
	defer req.lock.Unlock()
	req.Pending[payload.ID] = payload;
	*reply = true;
	return nil;
}

func (req *VoteResponses) Listen(ID int, reply *VoteResponse) error {
	// fmt.Println("Listening for Vote response");
	req.lock.Lock()
	defer req.lock.Unlock()
	if Vote, exists := req.Pending[ID]; exists {
		*reply = Vote;
	} else {
		*reply = VoteResponse{
			Term: -1,
			Vote: false,
			ID: -1,
		}
	}
	return nil;
} 
type AppendEntryRequest struct { 
	Term int
	LeaderId int
	ClientId int
	PrevLogIndex int
	Entry LogEntry
	LeaderCommit int
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
	// fmt.Println("Adding append Entry request");
	req.lock.Lock()
	defer req.lock.Unlock()
	req.Pending[payload.ClientId] = payload;
	*reply = true;
	return nil;
}

func (req *AppendEntryRequests) Listen(ClientId int, reply *AppendEntryRequest) error {
	// fmt.Println("Listening for append Entry request");
	req.lock.Lock()
	defer req.lock.Unlock()
	if _, ok := req.Pending[ClientId]; ok {
		*reply = req.Pending[ClientId];
		delete(req.Pending, ClientId);
		return nil;
	}
	*reply = AppendEntryRequest{
		Term: -1,
		LeaderId: -1,
		ClientId: -1,
		PrevLogIndex: -1,
		Entry: LogEntry{
			Term: -1,
			Command: "",
		},
		LeaderCommit: -1,
	}
	return nil;
}

type AppendEntryResponse struct {
	Term int
	ID int
	Success bool
}

type AppendEntryResponses struct {
	Pending map[int]AppendEntryResponse
	lock sync.Mutex
}

func NewAppendEntryResponses() *AppendEntryResponses {
	return &AppendEntryResponses{
		Pending: make(map[int]AppendEntryResponse),
	}
}

func (resp *AppendEntryResponses) Add(payload AppendEntryResponse, reply *bool) error {
	// fmt.Println("Adding append Entry response");
	resp.lock.Lock()
	defer resp.lock.Unlock()
	resp.Pending[payload.ID] = payload;
	*reply = true;
	return nil;
}

func (resp *AppendEntryResponses) Listen(ID int, reply *AppendEntryResponse) error {
	// fmt.Println("Listening for append Entry response");
	resp.lock.Lock()
	defer resp.lock.Unlock()
	if _, ok := resp.Pending[ID]; ok {
		*reply = resp.Pending[ID];
		delete(resp.Pending, ID);
		return nil;
	}
	*reply = AppendEntryResponse{
		Term: -1,
		ID: -1,
		Success: false,
	}
	return nil;
}