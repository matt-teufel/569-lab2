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
	X_TIME     = 2
	Y_TIME     = 4
	Z_TIME_MAX = 60
	Z_TIME_MIN = 40
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
	var neighborMembership shared.Membership
	if err := server.Call("Requests.Listen", id, &neighborMembership); err != nil { 
		fmt.Println("Failed Read Messages error:  ", err)
	} 
	return &neighborMembership
}

func calcTime() float64 {
	//TODO
	return float64(time.Now().UnixNano()) / float64(time.Second)
}

var wg = &sync.WaitGroup{}

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

	time.AfterFunc(time.Second*X_TIME, func() { runAfterX(server, &self_node, &membership, id) })
	time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(server, neighbors, &membership, id) })
	time.AfterFunc(time.Second*time.Duration(Z_TIME), func() { runAfterZ(server, id) })

	wg.Add(1)
	wg.Wait()
}

func runAfterX(server *rpc.Client, node *shared.Node, membership **shared.Membership, id int) {
	//TODO
	// fmt.Print("calling membership update\n")
	currTime:= calcTime()
	self_node.Hbcounter += 1;
	self_node.Time = currTime;
	var reply shared.Node
	printMembership(**membership)
	if err := server.Call("Membership.Update", self_node, &reply); err != nil { 
		fmt.Println("Failed: Membership.Update() error: ", err)
	}
	printMembership(**membership)
	time.AfterFunc(time.Second*X_TIME, func() { runAfterX(server, &self_node, membership, id) })
}

func runAfterY(server *rpc.Client, neighbors [2]int, membership **shared.Membership, id int) {
	var neighborMembership = readMessages(*server, self_node.ID, **membership);
	fmt.Println("combining tables")
	var combinedTable = shared.CombineTables(*membership, neighborMembership)
	fmt.Println("done combining")
	*membership = combinedTable;
	var currTime float64 = calcTime();
	//kill nodes that have not been incrased or had time update 
	for _, node := range((*membership).Members) {
		fmt.Println("time difference: ", currTime - node.Time);
		if(node.Alive && (currTime - node.Time) > ( (MAX_NODES-1) * Y_TIME)) { 
			node.Alive = false;
			var reply shared.Node;
			if err:= server.Call("Membership.Update", node, &reply); err != nil { 
				fmt.Println("Failed: Membership.Update() killing error: ", err)
			}
		}
	}
	printMembership(**membership)	
	sendMessage(*server, neighbors[0], **membership)
	sendMessage(*server, neighbors[1], **membership)
	time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(server, neighbors, membership, id) })
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
