package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	pb "project-root/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	/*
		clientID, _ := strconv.Atoi(os.Args[1])
		clientPorts := []string{"50061", "50062", "50063"}

		listener, _ := net.Listen("tcp", "localhost:"+clientPorts[clientID])
		clientServe := grpc.NewServer()
	*/

	nodeAddresses := []string{"50061", "50062", "50063"}
	connection, _ := grpc.NewClient("localhost:"+nodeAddresses[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer connection.Close()

	client := pb.NewClientClient(connection)

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Ready")
	for true {
		scanner.Scan()
		var cmd []string = strings.Split(scanner.Text(), " ")
		if cmd[0] == ".e" || cmd[0] == ".exit" {
			break
		} else if strings.ToLower(cmd[0]) == "bid" {
			// With timeout bullshit
			ctx := context.Background()
			bid, _ := strconv.Atoi(cmd[1])
			mu.Lock()
			LogicalTime(0)
			curTime := logicalTime
			mu.Unlock()
			fmt.Println("About to bid")
			bidAck, err := client.Bid(ctx, &pb.Bid{Bid: int32(bid), Id: 1, Timestamp: curTime})
			if err != nil {
				fmt.Println("Got en err")
			}
			mu.Lock()
			LogicalTime(bidAck.Timestamp)
			mu.Unlock()
			fmt.Println(bidAck.Outcome)
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
