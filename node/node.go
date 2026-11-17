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

type NodePool struct {
	pb.UnimplementedNodeServer
	nodeID      int32                   // between 0-2
	logicalTime int32                   // the node's current time
	connections map[int32]pb.NodeClient // the other nodes
	mu          sync.Mutex
	isLeader    bool
}

type ClientPool struct {
	pb.UnimplementedClientServer
	nodeID      int32                     // between 0-2
	connections map[int32]pb.ClientClient // the other nodes
	mu          sync.Mutex
}

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

func (p *NodePool) LogicalTime(otherTime int32) int32 {
	p.RecievedLogicalTime(otherTime)
	p.logicalTime++

	return p.logicalTime
}

func (p *NodePool) RecievedLogicalTime(time int32) {
	if time > p.logicalTime {
		p.logicalTime = time
	}
}

func (p *NodePool) connectToNode(connection pb.NodeClient) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p.mu.Lock()
	p.LogicalTime(0)
	log.Println(p.nodeID, "sent election request at time", p.logicalTime)
	result, err := connection.NodeJoin(ctx, &pb.Election{Timestamp: p.logicalTime})
	p.mu.Unlock()
	if err == nil {
		p.mu.Lock()
		p.LogicalTime(result.Timestamp)
		log.Println(p.nodeID, "got timestamp back from a node with lower id at time", p.logicalTime)
		p.mu.Unlock()
	}
}

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
		time.Sleep(100 * time.Millisecond)
		//empty on purpose
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

var sentElections int
var electionResponses int
var ongoingElectionId int
var biggerNodeExists bool
var tempLock sync.Mutex

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

	for {
		fmt.Println("Enter message to leave")
		if bufio.NewScanner(os.Stdin).Scan() {
			return
		}
	}
}
