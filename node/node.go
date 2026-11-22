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

// Called from a client
func (p *NodePool) Bid(ctx context.Context, bid *pb.Bid) (*pb.Acknowledgement, error) {
	log.Println("Someone tried to bid")
	if p.isLeader {
		return p.LeaderBid(ctx, bid)
	} else {
		log.Println("Node", p.nodeID, "Tried to call to leader")
		curLead := p.connections[p.currentLeader]
		log.Println("Found current leader")
		log.Println("curLead is", curLead)
		log.Println("Node", p.nodeID, "is forwarding bid to leader")
		res, err := curLead.NodeBid(ctx, bid)
		if err != nil {
			log.Println("Node", p.nodeID, "got the following error: \"", err.Error(), "\"")
			p.callElection()
			if p.isLeader {
				return p.LeaderBid(ctx, bid)
			} else {
				curLead := p.connections[p.currentLeader]
				return curLead.NodeBid(ctx, bid)
			}
		}
		return res, err
	}
}

// Called from another node
func (p *NodePool) NodeBid(ctx context.Context, bid *pb.Bid) (*pb.Acknowledgement, error) {
	log.Println("Node", p.nodeID, "recieved a forwarded bid")
	return p.LeaderBid(ctx, bid)
}

// Called from this node
func (p *NodePool) LeaderBid(ctx context.Context, bid *pb.Bid) (*pb.Acknowledgement, error) {
	log.Println("The leader was asked for a bid")
	if p.isDone || p.logicalTime > 100 {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.LogicalTime(bid.Timestamp)
		if !p.isDone {
			log.Println("The bidding has just finished at time", p.logicalTime)
		}
		p.isDone = true
		return &pb.Acknowledgement{Outcome: "bidding over", Timestamp: p.logicalTime}, nil
	} else if bid.Bid > p.highestBid {
		log.Println("The bid was higher than the previous highest bid")
		p.mu.Lock()
		p.highestBid = bid.Bid
		p.winningName = bid.Name
		log.Println("Node", p.nodeID, "about to write to nodes at time", p.logicalTime)
		p.mu.Unlock()
		p.WriteToNodes()
		p.mu.Lock()
		log.Println("Node", p.nodeID, "done writing to nodes at time", p.logicalTime)
		defer p.mu.Unlock()
		p.LogicalTime(bid.Timestamp)
		log.Println("Node", p.nodeID, "about to return acknowledgement at time", p.logicalTime)
		return &pb.Acknowledgement{Outcome: "successful bid", Timestamp: p.logicalTime}, nil
	} else {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.LogicalTime(bid.Timestamp)
		log.Println("Node", p.nodeID, "about to return acknowledgement at time", p.logicalTime)
		return &pb.Acknowledgement{Outcome: "unsuccessful bid", Timestamp: p.logicalTime}, nil
	}
}

// Called from a client
func (p *NodePool) Result(ctx context.Context, resultRequest *pb.ResultRequest) (*pb.ResultResponse, error) {
	if p.isDone {
		return &pb.ResultResponse{
			HighestBid: p.highestBid,
			WinnerName: p.winningName,
			IsFinished: true,
		}, nil
	} else {
		return &pb.ResultResponse{HighestBid: p.highestBid}, nil
	}
}

// All rpc calls and communication internally between nodes.

type NodePool struct {
	pb.UnimplementedNodeServer
	pb.UnimplementedClientServer
	nodeID        int32                   // between 0-2
	logicalTime   int32                   // the node's current time
	connections   map[int32]pb.NodeClient // the other nodes
	mu            sync.Mutex
	isLeader      bool
	highestBid    int32
	currentLeader int32
	winningName   string
	isDone        bool
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
	p.mu.Lock()
	log.Println("Node", p.nodeID, "has recieved at write at time", p.logicalTime)
	p.isDone = status.IsDone
	p.highestBid = status.HighestBid
	p.winningName = status.WinningName
	p.currentLeader = status.CurrentLeader
	p.LogicalTime(status.Timestamp)
	p.mu.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	p.LogicalTime(0)
	log.Println("Node", p.nodeID, "is sending acknowledgement of write at time", p.logicalTime)
	return &pb.Acknowledgement{Timestamp: p.logicalTime}, nil
}

// Called from this node
func (p *NodePool) WriteToNodes() {
	//time.Sleep(time.Second)
	tempNodesLock.Lock()
	tempNodesResponded = 0
	tempNodesLock.Unlock()
	amountOfConnections := len(p.connections)
	for id, connection := range p.connections {
		log.Println("Node", p.nodeID, "writing to node", id)
		go p.WriteToNodesTheCoolOne(connection, id)
	}

	for tempNodesResponded < amountOfConnections {
		// Do nothing, wait
		time.Sleep(30 * time.Millisecond)
	}
}

var tempNodesResponded int
var tempNodesLock sync.Mutex

// Called from this node
func (p *NodePool) WriteToNodesTheCoolOne(connection pb.NodeClient, id int32) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	p.mu.Lock()
	p.LogicalTime(0)
	curLogicalTime := p.logicalTime
	p.mu.Unlock()
	ack, err := connection.Write(ctx, &pb.Status{IsDone: p.isDone, HighestBid: p.highestBid, WinningName: p.winningName, Timestamp: curLogicalTime, CurrentLeader: p.currentLeader})
	if err != nil {
		log.Println("Node", p.nodeID, "write request returned error: \"", err.Error(), "\"")
		p.mu.Lock()
		//delete(p.connections, id)
		p.LogicalTime(0)
		p.mu.Unlock()
	} else {
		p.mu.Lock()
		p.LogicalTime(ack.Timestamp)
		log.Println("Node", p.nodeID, "recieved acknowledgement of write from", id, "at time", p.logicalTime)
		p.mu.Unlock()
	}
	tempNodesLock.Lock()
	tempNodesResponded++
	tempNodesLock.Unlock()
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
	log.Println("Node", p.nodeID, "sent connection request at time", p.logicalTime)
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
		log.Println("Node", p.nodeID, "looking at connection with id ", id)
		if id < p.nodeID {
			log.Println(id, "connection has smaller id")
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
		log.Println("Node", p.nodeID, "looking at connection with id ", id)
		if id > p.nodeID {
			log.Println(id, "connection has bigger id")
			tempLock.Lock()
			sentElections++
			tempLock.Unlock()
			go p.sendElection(connection)
		}
	}
	log.Println("Node", p.nodeID, "sent election to all connections")
	for sentElections > electionResponses && !biggerNodeExists {
		time.Sleep(50 * time.Millisecond)
		// used for waiting until other nodes respond
	}
	log.Println("Node", p.nodeID, "has recieved enough responses from the election")
	if !biggerNodeExists {
		p.mu.Lock()
		p.LogicalTime(p.logicalTime)
		log.Println("Node", p.nodeID, "has become leader at time", p.logicalTime)
		p.isLeader = true
		p.currentLeader = p.nodeID
		p.mu.Unlock()
	}
	log.Println("Node", p.nodeID, "isLeader variable is set to", p.isLeader, "at time", p.logicalTime)
	tempLock.Lock()
	ongoingElectionId++
	tempLock.Unlock()
	log.Println("ongoingElectionId is", ongoingElectionId, "at time", p.logicalTime)
	if p.isLeader {
		log.Println("Node", p.nodeID, "is sending write to nodes after becoming leader at time", p.logicalTime)
		p.WriteToNodes()
	}
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

	listener, _ := net.Listen("tcp", "localhost:"+nodePorts[id])

	go server.Serve(listener)

	//time.Sleep(time.Second)

	fmt.Println("Listening on", "localhost:"+nodePorts[id])

	for peerID, port := range nodePorts {
		if peerID == id {
			continue
		}
		connection, _ := grpc.NewClient("localhost:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
		defer connection.Close()
		pool.connections[int32(peerID)] = pb.NewNodeClient(connection)
	}

	pool.joinNodePool()

	//time.Sleep(time.Second)

	pool.callElection()

	// TODO: Set up client server
	clientServer := grpc.NewServer()
	pb.RegisterClientServer(clientServer, pool)
	clientPorts := []string{"50061", "50062", "50063"}

	clientListener, _ := net.Listen("tcp", "localhost:"+clientPorts[id])

	go clientServer.Serve(clientListener)

	//TODO: heartbeat, if leader does not respond to heartbeat, then leader is dead and new election

	for {
		fmt.Println("Enter message to leave")
		if bufio.NewScanner(os.Stdin).Scan() {
			return
		}
	}
}
