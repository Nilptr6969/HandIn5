package main

import (
	"context"
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

func (p *NodePool) callElection() {
	sentElections = 0
	electionResponses = 0
	biggerNode = false
	for id, connection := range p.connections {
		if id > p.nodeID {
			sentElections++
			go p.sendElection(connection)
		}
	}
	for sentElections < electionResponses && !biggerNode {
		//empty on purpose
	}
	if !biggerNode {
		p.isLeader = true
	}
	ongoingElectionId++
}

func (p *NodePool) sendElection(connection pb.NodeClient) {
	thisElectionId := ongoingElectionId
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := connection.Election(ctx, &pb.Election{Timestamp: p.logicalTime})
	if ongoingElectionId != thisElectionId {
		return
	}
	if err != nil {
		p.mu.Lock()
		electionResponses++
		p.mu.Unlock()
	} else {
		p.mu.Lock()
		biggerNode = true
		p.mu.Unlock()
	}
	//make logical time correctly, not this
	p.logicalTime = result.Timestamp
}

var sentElections int
var electionResponses int
var ongoingElectionId int
var biggerNode bool

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

	server.Serve(listener)
}
