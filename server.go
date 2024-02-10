package main

import (
	"io"
	"lab2/shared"
	"net/http"
	"net/rpc"
)

func main() {
	// create a Membership list
	nodes := shared.NewMembership()
	requests := shared.NewRequests()
	VoteRequests := shared.NewVoteRequests()
	VoteResponses := shared.NewVoteResponses()
	appendEntryRequests := shared.NewAppendEntryRequests()
	appendEntryResponses := shared.NewAppendEntryResponses()

	// register nodes with `rpc.DefaultServer`
	rpc.Register(nodes)
	rpc.Register(requests)
	rpc.Register(VoteRequests)
	rpc.Register(VoteResponses)
	rpc.Register(appendEntryRequests)
	rpc.Register(appendEntryResponses)

	// register an HTTP handler for RPC communication
	rpc.HandleHTTP()

	// sample test endpoint
	http.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
		io.WriteString(res, "RPC SERVER LIVE!")
	})

	// listen and serve default HTTP server
	http.ListenAndServe("localhost:9005", nil)
}
