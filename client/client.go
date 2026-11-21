package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "project-root/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	mu.Lock()
	nodeAddresses = []string{"50061", "50062", "50063"}
	NewConnection()
	mu.Unlock()

	defer connection.Close()

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Ready")
	for true {
		scanner.Scan()
		var cmd []string = strings.Split(scanner.Text(), " ")
		if cmd[0] == ".e" || cmd[0] == ".exit" {
			break
		} else if strings.ToLower(cmd[0]) == "bid" {
			bid, _ := strconv.Atoi(cmd[1])
			Bid(bid)
		} else if strings.ToLower(cmd[0]) == "result" {
			// With timeout bullshit
			ctx := context.Background()
			mu.Lock()
			LogicalTime(0)
			result, _ := client.Result(ctx, &pb.ResultRequest{Timestamp: logicalTime})
			LogicalTime(result.Timestamp)
			mu.Unlock()
			if result.IsFinished {
				fmt.Println("The winner is", result.WinnerId, "with a bid of", result.HighestBid)
			} else {
				fmt.Println("The current highest bid is:", result.HighestBid)
			}
		}

	}
}

func NewConnection() {
	if connection != nil {
		connection.Close()
	}
	time.Sleep(100 * time.Millisecond)
	connection, _ = grpc.NewClient("localhost:"+nodeAddresses[len(nodeAddresses)-1], grpc.WithTransportCredentials(insecure.NewCredentials()))
	client = pb.NewClientClient(connection)
}

func Bid(bid int) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	mu.Lock()
	LogicalTime(0)
	curTime := logicalTime
	mu.Unlock()
	fmt.Println("About to bid")
	bidAck, err := client.Bid(ctx, &pb.Bid{Bid: int32(bid), Id: 1, Timestamp: curTime})
	if err != nil {
		fmt.Println(err.Error())
		if len(nodeAddresses) <= 1 {
			fmt.Println("No nodes left")
			return
		}
		mu.Lock()
		nodeAddresses = nodeAddresses[:len(nodeAddresses)-1]
		NewConnection()
		mu.Unlock()
		Bid(bid)
	} else {
		mu.Lock()
		LogicalTime(bidAck.Timestamp)
		mu.Unlock()
		fmt.Println(bidAck.Outcome)
	}
}

var nodeAddresses []string
var connection *grpc.ClientConn
var client pb.ClientClient
var logicalTime int32
var mu sync.Mutex

// Logical time
func LogicalTime(otherTime int32) int32 {
	RecievedLogicalTime(otherTime)
	logicalTime++

	return logicalTime
}

// Logical time
func RecievedLogicalTime(time int32) {
	if time > logicalTime {
		logicalTime = time
	}
}
