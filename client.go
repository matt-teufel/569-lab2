package main

import (
	"fmt"
	"lab2/shared"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	MAX_NODES  = 8
	X_TIME     = 200
	Y_TIME     = 500
	Z_TIME_MAX = 10000
	Z_TIME_MIN = 4000
	ELECTION_TIMEOUT = 3 * Y_TIME; // make RAFT intervals proportional to neighbor protocol sends 
	MIN_JITTER = Y_TIME
	MAX_JITTER = 2 * Y_TIME 
	ELECTION_DURATION = Y_TIME * 10
	FOLLOWER = "follower"
	CANDIDATE = "candidate"
	LEADER = "leader"

)

var self_node shared.Node

// Send the current membership table to a neighboring node with the provided ID
func sendMessage(server rpc.Client, id int, membership shared.Membership) {
//TODO
	req := shared.Request{
		ID:    id,
		Table: membership,
		}
	var success bool
	if err := server.Call("Requests.Add", req, &success); err != nil { 
		fmt.Println("Failed Send Message error:  ", err)
	}
}

// Read incoming messages from other nodes
func readMessages(server rpc.Client, id int, membership shared.Membership) *shared.Membership {
	//TODO
	var m shared.Membership;
	if err := server.Call("Requests.Listen", id, &m); err != nil { 
		fmt.Println("Failed Read Messages error:  ", err)
	} 
	return &m;
}

func calcTime() float64 {
	//TODO
	return float64(time.Now().UnixNano()) / float64(time.Second)
}

var wg = &sync.WaitGroup{}

var globalLock sync.Mutex;

func main() {
	rand.Seed(time.Now().UnixNano())
	Z_TIME := rand.Intn(Z_TIME_MAX-Z_TIME_MIN) + Z_TIME_MIN

	// Connect to RPC server
	server, _ := rpc.DialHTTP("tcp", "localhost:9005")

	args := os.Args[1:]

	// Get ID from command line argument
	if len(args) == 0 {
		fmt.Println("No args given")
		return
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("Found Error", err)
	}

	fmt.Println("Node", id, "will fail after", Z_TIME, "seconds")

	currTime := calcTime()
	// Construct self
	self_node = shared.Node{ID: id, Hbcounter: 0, Time: currTime, Alive: true}
	var self_node_response shared.Node // Allocate space for a response to overwrite this

	// Add node with input ID
	if err := server.Call("Membership.Add", self_node, &self_node_response); err != nil {
		fmt.Println("Error:2 Membership.Add()", err)
	} else {
		fmt.Printf("Success: Node created with id= %d\n", id)
	}

	neighbors := self_node.InitializeNeighbors(id, MAX_NODES)
	fmt.Println("Neighbors:", neighbors)

	membership := shared.NewMembership()
	var reply shared.Node;
	membership.Add(self_node, &reply);

	sendMessage(*server, neighbors[0], *membership)
	sendMessage(*server, neighbors[1], *membership)

	// crashTime := self_node.CrashTime()

	time.AfterFunc(time.Millisecond*X_TIME, func() { runAfterX(server, &self_node, membership, id) })
	time.AfterFunc(time.Millisecond*Y_TIME, func() { runAfterY(server, neighbors, membership, id) })
	time.AfterFunc(time.Millisecond*time.Duration(Z_TIME), func() { runAfterZ(server, id) })

	wg.Add(1)
	wg.Wait()
}

func runAfterX(server *rpc.Client, node *shared.Node, membership *shared.Membership, id int) {
	// START NEIGHBOR PROTOCOL 
	globalLock.Lock();
	currTime:= calcTime()
	node.Hbcounter += 1;
	node.Time = currTime;
	var reply shared.Node;
	membership.Update(*node, &reply);
	if err := server.Call("Membership.Update", node, &reply); err != nil { 
		fmt.Println("Failed: Membership.Update() error: ", err)
	}
	// END NEIGHBOR PROTOCOL

	time.AfterFunc(time.Millisecond*X_TIME, func() { runAfterX(server, node, membership, id) })
	globalLock.Unlock();
}

func runAfterY(server *rpc.Client, neighbors [2]int, membership *shared.Membership, id int) {
	// START NEIGHBOR PROTOCOL 
	globalLock.Lock();
	// read any requests from neighbors 
	neighborMembership := readMessages(*server, id, *membership);
	var combinedTable = shared.CombineTables(membership, neighborMembership)
	membership = combinedTable;
	// read state from server 
	server.Call("Membership.Read", id, neighborMembership);
	fmt.Println("SERVER STATE");
	printMembership(*neighborMembership)
	fmt.Println("END SERVER STATE");
	combinedTable = shared.CombineTables(membership, neighborMembership);
	membership = combinedTable;
	var currTime float64 = calcTime();
	// kill nodes that have not been incrased or had time update 
	for _, node := range((*membership).Members) {
		// fmt.Println("time difference: ", currTime - node.Time);
		if(node.Alive && (currTime - node.Time) > ELECTION_TIMEOUT) { 
			node.Alive = false;
			var reply shared.Node;
			membership.Update(node, &reply);
			if err:= server.Call("Membership.Update", node, &reply); err != nil { 
				fmt.Println("Failed: Membership.Update() killing error: ", err)
			}
		}
	}
	printMembership(*membership)	
	sendMessage(*server, neighbors[0], *membership)
	sendMessage(*server, neighbors[1], *membership)
	//END NEIGHBOR PROTOCOL 


	time.AfterFunc(time.Millisecond*Y_TIME, func() { runAfterY(server, neighbors, membership, id) })
	globalLock.Unlock();
}

func runAfterZ(server *rpc.Client, id int) {
	//TODO
	fmt.Printf("Node %d has failed\n", id)
	os.Exit(0)
}

func printMembership(m shared.Membership) {
	for _, val := range m.Members {
		status := "is Alive"
		if !val.Alive {
			status = "is Dead"
		}
		fmt.Printf("Node %d has hb %d, time %.1f and %s\n", val.ID, val.Hbcounter, val.Time, status)
	}
	fmt.Println("")
}

func printNode(n shared.Node) { 
	fmt.Printf("Printing node in run after X %d has hb %d, time %.1f and %s\n", n.ID, n.Hbcounter, n.Time, n.Alive);
}

func candidateWaitTime() int { 
	return rand.Intn(MAX_JITTER-MIN_JITTER) + MIN_JITTER
}
func isLeaderAlive(membership shared.Membership, raftMembership shared.RaftMembership) bool {	
	for _, node := range(raftMembership.Members) {
		if node.state == LEADER && membership.Members[node.id].Alive {
			return true
		}
	}
	return false
}

func electionInterval(server * rpc.Client, membership * shared.Membership, raftMembership * shared.RaftMembership, id int) { 
	if (!isLeaderAlive(membership, raftMembership)) {
		// wait for random time
		waitTime :=  candidateWaitTime()
		fmt.Println("Waiting to start election with time: ", waitTime)
		time.Sleep(time.Millisecond  * time.Duration(waitTime));
		// if still haven't received any message from leader, and we also haven't received any
		// vote requests from a different higher term candidate, then we become the canidate
		// and we will request votes 
		if ( !isLeaderAlive(membership, raftMembership)) {
			fmt.Println("Leader is still dead, starting eleciton") 
			// become a candidate 

		}
		var success bool
		if err := server.Call("Election.Start", 0, &success); err != nil {
			fmt.Println("Failed Election.Start() error: ", err)
		}
	}
}