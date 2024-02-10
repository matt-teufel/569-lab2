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
	X_TIME     = 1500
	Y_TIME     = 3000
	Z_TIME_MAX = 5000000
	Z_TIME_MIN = 2000000
	ELECTION_TIMEOUT = 3 * Y_TIME; // make RAFT intervals proportional to neighbor protocol sends 
	MIN_JITTER = Y_TIME
	MAX_JITTER = 2 * Y_TIME 
	ELECTION_DURATION = Y_TIME * 5
	MESSAGE_INTERVAL = X_TIME / 4
	FOLLOWER = "follower"
	CANDIDATE = "candidate"
	LEADER = "leader"

)

var self_node shared.Node
var currentLeader int = -1;
var appendCount int = 1;
var raftLock sync.Mutex;

// Send the current membership table to a neighboring node with the provided ID
func sendMessage(server rpc.Client, id int, membership shared.Membership) {
//TODO
	req := shared.Request{
		ID:    id,
		Table: membership,
		}
	var Success bool
	if err := server.Call("Requests.Add", req, &Success); err != nil { 
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

	// Get ID from Command line argument
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
	// time.AfterFunc(time.Millisecond*time.Duration(Z_TIME), func() { runAfterZ(server, id) })

	self_raft_node := shared.NewRaftNode(id);
	time.AfterFunc(time.Millisecond * MESSAGE_INTERVAL, func() { messageInterval(server, membership, self_raft_node) })
	time.AfterFunc(time.Millisecond*ELECTION_TIMEOUT, func() { electionInterval(server, membership, self_raft_node) })

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
	raftLock.Lock();
	defer raftLock.Unlock();
	*membership = *combinedTable;
	// read State from server 
	server.Call("Membership.Read", id, neighborMembership);
	// fmt.Println("SERVER STATE");
	// printMembership(*neighborMembership)
	// fmt.Println("END SERVER STATE");
	combinedTable = shared.CombineTables(membership, neighborMembership);
	*membership = *combinedTable;
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
			// printMembership(membership);
			currentLeader = -1;
		}
	}
	return false 
}

func sendAppendEntryRequest(server rpc.Client, membership shared.Membership, rn shared.RaftNode, 
	Entry shared.LogEntry) { 
	// fmt.Println("Printing raft node in send entry");
	// printRaftNodeState(rn);
	for _, node := range membership.Members { 
		if (node.Alive && node.ID != rn.ID) { 
			req := shared.AppendEntryRequest{
				Term: rn.CurrentTerm,
				LeaderId: rn.ID,
				ClientId: node.ID,
				Entry: Entry,
				LeaderCommit: rn.CommitIndex,
			}
			var Success bool;
			if err := server.Call("AppendEntryRequests.Add", req, &Success); err != nil {
				fmt.Println("Failed: AppendEntryRequests.Add() error: ", err)
			}
		}
	}
}

func sendAppendEntryResponse(server rpc.Client, Term int, ClientId int, Success bool)  { 
	resp := shared.AppendEntryResponse{
		Term: Term,
		ID: ClientId,
		Success: Success,
	}
	var reply bool;
	if err := server.Call("AppendEntryResponses.Add", resp, &reply); err != nil {
		fmt.Println("Failed: AppendEntryResponses.Add() error: ", err)
	}
}

func listenAppendEntryRequest(server rpc.Client, ClientId int)  shared.AppendEntryRequest { 
	var reply shared.AppendEntryRequest;
	if err := server.Call("AppendEntryRequests.Listen", ClientId, &reply); err != nil {
		fmt.Println("Failed: AppendEntryRequests.Listen() error: ", err)
	}
	return reply;
}

func listenAppendEntryResponse(server rpc.Client, ClientId int)  shared.AppendEntryResponse { 
	var reply shared.AppendEntryResponse;
	if err := server.Call("AppendEntryResponses.Listen", ClientId, &reply); err != nil {
		fmt.Println("Failed: AppendEntryResponses.Listen() error: ", err)
	}
	return reply;
}

func sendVoteRequest(server rpc.Client, membership shared.Membership, rn shared.RaftNode ) { 
	// fmt.Println("In send vote request")
	// printMembership(membership)
	for _, node := range membership.Members {
		if (node.Alive && node.ID!= rn.ID) {
			req := shared.VoteRequest{
				Term: rn.CurrentTerm + 1,
				LeaderId: rn.ID,
				ClientId: node.ID,
				LastLogIndex: rn.CommitIndex,
				LastLogTerm: rn.CurrentTerm,
			}
			var Success bool;
			// fmt.Println("Sending vote request to ", node.ID)
			if err := server.Call("VoteRequests.Add", req, &Success); err != nil {
				fmt.Println("Failed: VoteRequests.Add() error: ", err)
			}
		}
	}
}

func listenVoteRequest(server rpc.Client, ClientId int) shared.VoteRequest { 
	var reply shared.VoteRequest;
	if err := server.Call("VoteRequests.Listen", ClientId, &reply); err != nil {
		fmt.Println("Failed: VoteRequests.Listen() error: ", err)
	}
	return reply;
}

func sendVoteResponse(server rpc.Client, Term int, ClientId int, Success bool)  {
	resp := shared.VoteResponse{
		Term: Term,
		ID: ClientId,
		Vote: Success,
	}
	var reply bool;
	if err := server.Call("VoteResponses.Add", resp, &reply); err != nil {
		fmt.Println("Failed: VoteResponses.Add() error: ", err)
	}
}
func listenVoteResponse(server rpc.Client, ClientId int) shared.VoteResponse {
	var reply shared.VoteResponse;
	if err := server.Call("VoteResponses.Listen", ClientId, &reply); err != nil {
		fmt.Println("Failed: VoteResponses.Listen() error: ", err)
	}
	return reply;
} 



func messageInterval(server * rpc.Client, membership *shared.Membership, rn * shared.RaftNode) {
	// fmt.Println("message interval")
	if (rn.State == CANDIDATE) { 
		time.AfterFunc(time.Millisecond * MESSAGE_INTERVAL, func() { messageInterval(server, membership, rn) })
		return; // we can ignore messages during our eleciton 
	}
	raftLock.Lock();
	defer raftLock.Unlock();
	// printRaftNodeState(*rn);
	if (currentLeader == rn.ID) { 
		// we are the leader, we want to continuously send append requests and responses 
		if (rn.CommitIndex == rn.AppendIndex) { 
			// we need to append a new Log Entry generate a new request 
			// fmt.Println("Sending a new append request")
			newLog := shared.LogEntry{Term:rn.CurrentTerm, Command:"new Command"}
			rn.AppendIndex++;
			rn.Log = append(rn.Log, newLog);
			sendAppendEntryRequest(*server, *membership, *rn, newLog);
		} else { 
			// we need to listen for AppendEntryResponses 
			for _, node := range(*membership).Members {
				if (node.Alive && node.ID!= rn.ID) {
					newAppendEntryResponse := listenAppendEntryResponse(*server, node.ID);
					if (newAppendEntryResponse.Term == rn.CurrentTerm) {
						// fmt.Println("Received a new append response")
						appendCount++;
					}
				}
			}
			if (appendCount > MAX_NODES/2)  {
				//we have received enough approvals, we can commit this Entry 
				rn.CommitIndex = rn.AppendIndex;
				appendCount = 1
			}
		}
	} else { 
		// we are a follower, we need to listen for append requests and send responses
		newAppendEntryRequest := listenAppendEntryRequest(*server, rn.ID);
		// fmt.Println("Received append entry request term: %d, LeaderId: %d, ClientId: %d LeadeerCommit: %d",
		//  newAppendEntryRequest.Term, newAppendEntryRequest.LeaderId, newAppendEntryRequest.ClientId, newAppendEntryRequest.LeaderCommit)	
		if (newAppendEntryRequest.Term >= rn.CurrentTerm) {
			if(currentLeader != newAppendEntryRequest.LeaderId) {
				// we have a new leader, update our leader
				fmt.Printf("We have a new leader, updating our leader to: %d\n", newAppendEntryRequest.LeaderId)
				currentLeader = newAppendEntryRequest.LeaderId;
			}
			// fmt.Println("Received a new append request, sending a response ")
			rn.CurrentTerm = newAppendEntryRequest.Term;
			rn.CommitIndex = rn.AppendIndex;
			rn.AppendIndex++;
			rn.Log = append(rn.Log, newAppendEntryRequest.Entry);
			sendAppendEntryResponse(*server, rn.CurrentTerm, 
				rn.ID, true);
		} 
	}
	time.AfterFunc(time.Millisecond * MESSAGE_INTERVAL, func() { messageInterval(server, membership, rn) })
}

func electionInterval(server * rpc.Client, membership * shared.Membership, rn * shared.RaftNode) { 
	// fmt.Println("election interval")
	raftLock.Lock();
	printRaftNodeState(*rn);
	if (!isLeaderAlive(*membership)) {
		// wait for random time
		waitTime :=  candidateWaitTime()
		fmt.Printf("The leader is dead, I will wait: %d ms before starting a new election\n", waitTime)
		raftLock.Unlock();
		time.Sleep(time.Millisecond  * time.Duration(waitTime));
		raftLock.Lock();
		// if still haven't received any message from leader, and we also haven't received any
		// Vote requests from a different higher Term candidate, then we become the canidate
		// and we will request Votes 
		if (!isLeaderAlive(*membership)) {
			// become a candidate 
			VoteRequest := listenVoteRequest(*server, rn.ID);
			// fmt.Printf("Vote request found: %d %d %d %d %d\n", VoteRequest.ClientId, VoteRequest.LeaderId, VoteRequest.Term, VoteRequest.LastLogIndex, VoteRequest.LastLogTerm);

			if(VoteRequest.Term == -1 || VoteRequest.LastLogIndex < rn.CommitIndex) { 
				// this means that this node is the first candidate to start an election or the 
				// current node running for election has out of date Log 
				fmt.Println("Making myself a candidate and starting an election")
				sendVoteRequest(*server, *membership, *rn);
				raftLock.Unlock()
				time.Sleep(time.Millisecond * ELECTION_DURATION);
				raftLock.Lock()
				// fmt.Println("totaling up all the Votes")
				var voteTotal int = 1;
				for _, node := range membership.Members {
					if (node.Alive && node.ID!= rn.ID) {
						voteResponse := listenVoteResponse(*server, node.ID);
						if (voteResponse.Vote) {
							voteTotal += 1;
						}
					}
				}
				// fmt.Println("total votes: ", voteTotal)
				if (voteTotal > MAX_NODES/2) {
					// we have majority approval, we become the leader
					rn.State = LEADER;
					rn.CurrentTerm++;
					currentLeader = rn.ID;
					fmt.Println("\nThe election is over, and I received a majority of votes, I am now the leader")
					println("\n :)")
					rn.AppendIndex = rn.CommitIndex;
				
				} else { 
					// we don't have enough Votes 
					rn.State = FOLLOWER;
					fmt.Println("I did not receive enough votes. I will become a follower again ")
				}
			    raftLock.Unlock();
			} else { 
				// someone else started an election, let's Vote for them
				fmt.Printf("Node %d already started an election, so I will vote for them isntead of becoming a candidate\n", VoteRequest.LeaderId);
				rn.VotedFor = VoteRequest.LeaderId;
				sendVoteResponse(*server, VoteRequest.Term, VoteRequest.ClientId, true);
				raftLock.Unlock();
			}
		} else {
			raftLock.Unlock()
		}
	} else {
		raftLock.Unlock()
	}
	time.AfterFunc(time.Millisecond*ELECTION_TIMEOUT, func() { electionInterval(server,membership, rn) })
}

func printRaftNodeState(rn shared.RaftNode) {
	fmt.Printf("Node %d is a %s, currentLeader: %d, CurrentTerm: %d, AppendIndex: %d, CommitIndex %d  \n",
	 rn.ID, rn.State, currentLeader, rn.CurrentTerm, rn.AppendIndex, rn.CommitIndex);
}
