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
	Z_TIME_MAX = 100000
	Z_TIME_MIN = 40000
	ELECTION_TIMEOUT = 3 * Y_TIME; // make RAFT intervals proportional to neighbor protocol sends 
	MIN_JITTER = Y_TIME
	MAX_JITTER = 2 * Y_TIME 
	ELECTION_DURATION = Y_TIME * 10
	MESSAGE_INTERVAL = X_TIME / 4
	FOLLOWER = "follower"
	CANDIDATE = "candidate"
	LEADER = "leader"

)

var self_node shared.Node
var currentLeader int = -1;
var appendCount int = 0;
var raftLock sync.Mutex;

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

	self_raft_node := shared.NewRaftNode(id);
	time.AfterFunc(time.Millisecond * MESSAGE_INTERVAL, func() { messageInterval(server, self_raft_node, 0) })
	time.AfterFunc(time.Millisecond*ELECTION_TIMEOUT, func() { electionInterval(server, self_raft_node, id) })

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
	// fmt.Println("SERVER STATE");
	// printMembership(*neighborMembership)
	// fmt.Println("END SERVER STATE");
	combinedTable = shared.CombineTables(membership, neighborMembership);
	membership = combinedTable;
	var currTime float64 = calcTime();
	// kill nodes that have not been incrased or had time update 
	for _, node := range((*membership).Members) {
		// fmt.Println("time difference: ", currTime - node.Time);
		if(node.Alive && (currTime - node.Time) > ELECTION_TIMEOUT/1000) { 
			node.Alive = false;
			var reply shared.Node;
			membership.Update(node, &reply);
			if err:= server.Call("Membership.Update", node, &reply); err != nil { 
				fmt.Println("Failed: Membership.Update() killing error: ", err)
			}
		}
	}
	// printMembership(*membership)	
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
func isLeaderAlive(membership shared.Membership) bool {	
	if currentLeader > -1 { 
		if membership.Members[currentLeader].Alive {
			return true
		} else { 
			currentLeader = -1;
		}
	}
	return false 
}

func sendAppendRequest(server rpc.Client, membership shared.Membership, rn shared.RaftNode, 
	entry shared.LogEntry) { 
	for _, node := range membership.Members { 
		if (node.Alive) { 
			req := shared.AppendRequest{
				term: rn.currentTerm,
				leaderId: rn.id,
				clientId: node.ID,
				prevLogTerm: rn.prevLogTerm,
				entry: entry,
				leaderCommit: rn.commitIndex,
			}
			var success bool;
			if err := server.Call("AppendRequests.Add", req, &success); err != nil {
				fmt.Println("Failed: AppendRequests.Add() error: ", err)
			}
		}
	}
}

func sendAppendResponse(server rpc.Client, term int, clientId int, success bool)  { 
	resp := shared.AppendResponse{
		term: term,
		clientId: clientId,
		success: success,
	}
	var reply bool;
	if err := server.Call("AppendResponses.Add", resp, &reply); err != nil {
		fmt.Println("Failed: AppendResponses.Add() error: ", err)
	}
}

func listenAppendRequest(server rpc.Client, clientId int) * shared.AppendEntryRequest { 
	var reply shared.AppendRequest;
	if err := server.Call("AppendRequests.Listen", clientId, &reply); err != nil {
		fmt.Println("Failed: AppendRequests.Listen() error: ", err)
	}
	return &reply;
}

func listenAppendResponse(server rpc.Client, clientId int) * shared.AppendEntriesResponse { 
	var reply shared.AppendResponse;
	if err := server.Call("AppendResponses.Listen", clientId, &reply); err != nil {
		fmt.Println("Failed: AppendResponses.Listen() error: ", err)
	}
	return &reply;
}

func sendVoteRequest(server rpc.Client, membership shared.Membership, rn shared.RaftNode ) { 
	for _, node := range membership.Members {
		if (node.Alive) {
			req := shared.VoteRequest{
				term: rn.currentTerm + 1,
				candidateId: rn.id,
				lastLogIndex: rn.commitIndex,
				lastLogTerm: rn.currentTerm,
			}
			var success bool;
			if err := server.Call("VoteRequests.Add", req, &success); err != nil {
				fmt.Println("Failed: VoteRequests.Add() error: ", err)
			}
		}
	}
}

func listenVoteRequest(server rpc.Client, clientId int) * shared.VoteRequest { 
	var reply shared.VoteRequest;
	if err := server.Call("VoteRequests.Listen", clientId, &reply); err != nil {
		fmt.Println("Failed: VoteRequests.Listen() error: ", err)
	}
	return &reply;
}

func sendVoteResponse(server rpc.Client, term int, clientId int, success bool)  {
	resp := shared.VoteResponse{
		term: term,
		clientId: clientId,
		success: success,
	}
	var reply bool;
	if err := server.Call("VoteResponses.Add", resp, &reply); err != nil {
		fmt.Println("Failed: VoteResponses.Add() error: ", err)
	}
}
func listenVoteResponse(server rpc.Client, clientId int) * shared.VoteResponse {
	var reply shared.VoteResponse;
	if err := server.Call("VoteResponses.Listen", clientId, &reply); err != nil {
		fmt.Println("Failed: VoteResponses.Listen() error: ", err)
	}
	return &reply;
} 



func messageInterval(server * rpc.Client, membership *shared.Membership, rn * shared.RaftNode) {
	fmt.Println("message interval")
	if (rn.state == CANDIDATE) { 
		return; // we can ignore messages during our eleciton 
	}
	raftLock.Lock();
	defer raftLock.Unlock();
	if (currentLeader == rn.id) { 
		// we are the leader, we want to continuously send append requests and responses 
		if (rn.commitIndex == rn.appendIndex) { 
			// we need to append a new log entry generate a new request 
			fmt.Println("Sending a new append request")
			newLog := shared.LogEntry{term:rn.currentTerm, command:"new command"}
			rn.appendIndex++;
			rn.prevLogTerm = rn.currentTerm;
			rn.log.append(newLog);
			sendAppendRequest(*server, *membership, *rn, newLog);
		} else { 
			// we need to listen for appendResponses 
			for _, node := range(*membership).Members {
				if (node.Alive) {
					newAppendResponse := listenAppendResponse(*server, node.ID);
					if (newAppendResponse.term == rn.currentTerm) {
						fmt.Println("Received a new append response")
						appendCount++;
					}
				}
			}
			if (appendCount > MAX_NODES/2)  {
				//we have received enough approvals, we can commit this entry 
				rn.commitIndex = rn.appendIndex;
				appendCount = 0
			}
		}
	} else { 
		// we are a follower, we need to listen for append requests and send responses
		newAppendRequest := listenAppendRequest(*server, rn.id);
		if (newAppendRequest.term >= rn.currentTerm) {
			if(currentLeader != newAppendRequest.leaderId) {
				// we have a new leader, update our leader
				fmt.Println("Updating to our new leader leader")
				currentLeader = newAppendRequest.leaderId;
			}
			fmt.Println("Received a new append request, sending a response ")
			rn.currentTerm = newAppendRequest.term;
			newAppendResponse := shared.AppendEntriesResponse{
				term: rn.currentTerm,
				success: true,
			}
			if (rn.appendIndex - rn.commitIndex > 1) { 
				rn.commitIndex++; // previous log entry must have got majority of votes for new one with hgiher index 
			}
			rn.appendIndex++;
			rn.log.append(newAppendRequest.entry);
			sendAppendResponse(*server, newAppendResponse.term, newAppendResponse.clientId, newAppendResponse.success);
		}
		
	}
}

func electionInterval(server * rpc.Client, membership * shared.Membership, rn * shared.RaftNode) { 
	fmt.Println("election interval")
	raftLock.Lock();
	defer raftLock.Unlock();
	if (!isLeaderAlive(membership)) {
		// wait for random time
		waitTime :=  candidateWaitTime()
		fmt.Println("Waiting to start election with time: ", waitTime)
		time.Sleep(time.Millisecond  * time.Duration(waitTime));
		// if still haven't received any message from leader, and we also haven't received any
		// vote requests from a different higher term candidate, then we become the canidate
		// and we will request votes 
		if (!isLeaderAlive(membership)) {
			// become a candidate 
			voteRequest := listenVoteRequest(*server, rn.id);
			if(voteRequest.term == -1 || voteRequest.lastLogIndex < rn.commitIndex) { 
				// this means that this node is the first candidate to start an election or the 
				// current node running for election has out of date log 
				fmt.Println("Starting election")
				sendVoteRequest(*server, *membership, *rn);
				time.Sleep(time.Millisecond * ELECTION_DURATION);
				fmt.Println("totaling up all the votes")
				var voteTotal int = 0;
				for _, node := range membership.Members {
					if (node.Alive) {
						voteResponse := listenVoteResponse(*server, node.ID);
						if (voteResponse.success) {
							voteTotal += 1;
						}
					}
				}
				if (voteTotal > (len(membership.Members)/2)) {
					// we have majority approval, we become the leader
					rn.state = LEADER;
					rn.currentTerm = voteRequest.term;
					currentLeader = rn.id;
					fmt.Println("I am the leader")
				} else { 
					// we don't have enough votes 
					rn.state = FOLLOWER;
					fmt.Println("Not enough votes, becoming a follower again ")
				}
			} else { 
				// someone else started an election, let's vote for them
				fmt.Println("voting for: ", voteRequest.candidateId);
				rn.votedFor = voteRequest.candidateId;
				sendVoteResponse(*server, voteRequest.term, voteRequest.candidateId, true);

			}
		}
	}
}

func printRaftNodeState(rn shared.RaftNode) {
	fmt.Printf("Node %d is a %s, currentLeader: %d, currentTerm: %d, appendIndex: %d, commitIndex %d  \n",
	 rn.id, rn.state, currentLeader, rn.currentTerm, rn.appendIndex, rn.commitIndex);
}
