package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	pb "project-root/grpc"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// All rpc calls and communication externally between the client and the server

type ClientPool struct {
	pb.UnimplementedClientServer
	nodeID      int32                     // between 0-2
	connections map[int32]pb.ClientClient // the other nodes
	mu          sync.Mutex
}

// Called from a client
func (p *ClientPool) Bid(ctx context.Context, bid *pb.Bid) (*pb.Acknowledgement, error) {
	// TODO: check if this node is the leader. If no, make a call to the leader, if yes, make a write to all followers

	// TODO: return acknowledgement to client
	return nil, nil
}

// Called from a client
func (p *ClientPool) Result(ctx context.Context, resultRequest *pb.ResultRequest) (*pb.ResultResponse, error) {
	// TODO: Check if auction is over.

	// TODO: return result to client
	return nil, nil
}

// All rpc calls and communication internally between nodes.

type NodePool struct {
	pb.UnimplementedNodeServer
	nodeID      int32                   // between 0-2
	logicalTime int32                   // the node's current time
	connections map[int32]pb.NodeClient // the other nodes
	mu          sync.Mutex
	isLeader    bool
}

// Called from another node
// Used when another node starts an election
func (p *NodePool) Election(ctx context.Context, input *pb.Election) (*pb.Election, error) {
	p.mu.Lock()
	p.LogicalTime(input.Timestamp)
	log.Println(p.nodeID, "received request for election at time", p.logicalTime)
	p.mu.Unlock()

	defer p.callElection()

	p.mu.Lock()
	defer p.mu.Unlock()
	p.LogicalTime(0)
	log.Println(p.nodeID, "returning response for election at time", p.logicalTime)
	return &pb.Election{Timestamp: p.logicalTime}, nil
}

// Called from another node
// Used when the leader is updating it's information
func (p *NodePool) Write(ctx context.Context, status *pb.Status) (*pb.Acknowledgement, error) {
	// TODO: Update all variables in status
	// Remember to lock the mutex

	// TODO: Send back acknowledgement
	return nil, nil
}

// Called from another node
// Used when another node with a higher id has joined the node pool
func (p *NodePool) NodeJoin(ctx context.Context, input *pb.Election) (*pb.Election, error) {
	p.mu.Lock()
	p.isLeader = false
	p.LogicalTime(input.Timestamp)
	log.Println(p.nodeID, "was told a node with a higher id joined at time", p.logicalTime)
	p.mu.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	p.LogicalTime(0)
	log.Println(p.nodeID, "returning timestamp to new node at time", p.logicalTime)
	return &pb.Election{Timestamp: p.logicalTime}, nil
}

// Internal functions for this node, used to contact other nodes

// Called from this node
// Used to contact nodes with lower id when this node joins the node pool
func (p *NodePool) connectToNode(connection pb.NodeClient) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p.mu.Lock()
	p.LogicalTime(0)
	log.Println(p.nodeID, "sent connection request at time", p.logicalTime)
	result, err := connection.NodeJoin(ctx, &pb.Election{Timestamp: p.logicalTime})
	p.mu.Unlock()
	if err == nil {
		p.mu.Lock()
		p.LogicalTime(result.Timestamp)
		log.Println(p.nodeID, "got timestamp back from a node with lower id at time", p.logicalTime)
		p.mu.Unlock()
	}
}

// Called from this node
// Used when this node is created/restored
func (p *NodePool) joinNodePool() {
	for id, connection := range p.connections {
		log.Println("looking at connection with id ", id)
		if id < p.nodeID {
			log.Println(id, "connection has bigger id")
			tempLock.Lock()
			sentElections++
			tempLock.Unlock()
			go p.connectToNode(connection)
		}
	}
}

// Called from this node
// Used start an election when a new node has joined, the leader has died, or someone else called an election
func (p *NodePool) callElection() {
	tempLock.Lock()
	sentElections = 0
	electionResponses = 0
	biggerNodeExists = false
	tempLock.Unlock()
	p.mu.Lock()
	p.LogicalTime(0)
	log.Println(p.nodeID, "starting election at time", p.logicalTime)
	log.Println("starting ongoingElectionId is ", ongoingElectionId)
	p.mu.Unlock()
	for id, connection := range p.connections {
		log.Println("looking at connection with id ", id)
		if id > p.nodeID {
			log.Println(id, "connection has bigger id")
			tempLock.Lock()
			sentElections++
			tempLock.Unlock()
			go p.sendElection(connection)
		}
	}
	log.Println("sent election to all connections")
	for sentElections > electionResponses && !biggerNodeExists {
		log.Println("waiting for responses")
		time.Sleep(50 * time.Millisecond)
		// used for waiting until other nodes respond
	}
	log.Println("enough responses received")
	if !biggerNodeExists {
		p.mu.Lock()
		p.LogicalTime(p.logicalTime)
		log.Println("I am the biggest node", p.nodeID, "at time", p.logicalTime)
		p.isLeader = true
		p.mu.Unlock()
	}
	log.Println("isLeader variable is set to", p.isLeader, "at time", p.logicalTime)
	tempLock.Lock()
	ongoingElectionId++
	tempLock.Unlock()
	log.Println("ongoingElectionId is", ongoingElectionId, "at time", p.logicalTime)
}

// Called from this node
// Used to contact other nodes regarding the ongoing election
func (p *NodePool) sendElection(connection pb.NodeClient) {
	thisElectionId := ongoingElectionId
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p.mu.Lock()
	p.LogicalTime(0)
	log.Println(p.nodeID, "sent election request at time", p.logicalTime)
	result, err := connection.Election(ctx, &pb.Election{Timestamp: p.logicalTime})
	p.mu.Unlock()
	if ongoingElectionId != thisElectionId {
		log.Println("This ongoing election is not this election id")
		return
	}
	if err != nil {
		// TODO: if we stop hardcoding connections, remove the connection from the connection pool.
		tempLock.Lock()
		electionResponses++
		tempLock.Unlock()
	} else {
		tempLock.Lock()
		biggerNodeExists = true
		tempLock.Unlock()

		p.mu.Lock()
		p.LogicalTime(result.Timestamp)
		log.Println("larger node has responded to message from", p.nodeID, "at time", p.logicalTime)
		p.mu.Unlock()
	}
	fmt.Println(biggerNodeExists)
}

// Temporary variables used for elections
var sentElections int
var electionResponses int
var ongoingElectionId int
var biggerNodeExists bool
var tempLock sync.Mutex

// Logical time
func (p *NodePool) LogicalTime(otherTime int32) int32 {
	p.RecievedLogicalTime(otherTime)
	p.logicalTime++

	return p.logicalTime
}

// Logical time
func (p *NodePool) RecievedLogicalTime(time int32) {
	if time > p.logicalTime {
		p.logicalTime = time
	}
}

// Setup
func main() {
	// Logging taken from https://last9.io/blog/write-logs-to-file/
	logFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	id, _ := strconv.Atoi(os.Args[1])

	// Setup for node server (the replicated servers)
	nodePorts := []string{"50051", "50052", "50053"}
	pool := &NodePool{nodeID: int32(id), logicalTime: 0, connections: make(map[int32]pb.NodeClient)}
	server := grpc.NewServer()
	pb.RegisterNodeServer(server, pool)

	for peerID, port := range nodePorts {
		if peerID == id {
			continue
		}
		connection, _ := grpc.NewClient("localhost:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
		defer connection.Close()
		pool.connections[int32(peerID)] = pb.NewNodeClient(connection)
	}

	listener, _ := net.Listen("tcp", "localhost:"+nodePorts[id])

	go server.Serve(listener)

	pool.joinNodePool()

	pool.callElection()

	// TODO: Set up client server

	for {
		fmt.Println("Enter message to leave")
		if bufio.NewScanner(os.Stdin).Scan() {
			return
		}
	}
}
