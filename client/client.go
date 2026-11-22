package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
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
	// Logging taken from https://last9.io/blog/write-logs-to-file/
	logFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	mu.Lock()
	nodeAddresses = []string{"50061", "50062", "50063"}
	NewConnection()
	mu.Unlock()

	defer connection.Close()

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("Enter a name: ")
	scanner.Scan()
	name = scanner.Text()

	fmt.Println("Welcome,", name)
	for true {
		scanner.Scan()
		var cmd []string = strings.Split(scanner.Text(), " ")
		if cmd[0] == ".e" || cmd[0] == ".exit" {
			break
		} else if strings.ToLower(cmd[0]) == "bid" {
			mu.Lock()
			log.Println("Client", name, "is attempting to make a bid at time", logicalTime)
			mu.Unlock()
			bid, _ := strconv.Atoi(cmd[1])
			fmt.Println("Attempting bid")
			Bid(bid)
			mu.Lock()
			log.Println("Client", name, "is finished with their attempt to make a bid at time", logicalTime)
			mu.Unlock()
		} else if strings.ToLower(cmd[0]) == "result" {
			mu.Lock()
			log.Println("Client", name, "is attempting to make a result request at time", logicalTime)
			mu.Unlock()

			fmt.Println("Requesting result")
			Result()

			mu.Lock()
			log.Println("Client", name, "is finished with their attempt to make a result request at time", logicalTime)
			mu.Unlock()
		} else {
			fmt.Println("Please enter a valid command")
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

func Result() {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	mu.Lock()
	LogicalTime(0)
	curTime := logicalTime
	mu.Unlock()
	result, err := client.Result(ctx, &pb.ResultRequest{Timestamp: curTime})
	if err != nil {
		mu.Lock()
		log.Println("Client", name, "result request returned with error: \"", err.Error(), "\"at time", logicalTime)
		mu.Unlock()
		if len(nodeAddresses) <= 1 {
			fmt.Println("No nodes left")
			return
		}
		mu.Lock()
		nodeAddresses = nodeAddresses[:len(nodeAddresses)-1]
		log.Println("Client", name, "is attempting to make a connection to a new node at time", logicalTime)
		NewConnection()
		log.Println("Client", name, "is attempting to resend result request at time", logicalTime)
		mu.Unlock()
		Result()
	} else {
		mu.Lock()
		LogicalTime(result.Timestamp)
		log.Println("Client", name, "has recieved result at time", logicalTime)
		mu.Unlock()
		if result.IsFinished {
			fmt.Println("The winner is", result.WinnerName, "with a bid of", result.HighestBid)
		} else {
			fmt.Println("The current highest bid is:", result.HighestBid)
		}
	}
}

func Bid(bid int) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	mu.Lock()
	LogicalTime(0)
	curTime := logicalTime
	mu.Unlock()
	bidAck, err := client.Bid(ctx, &pb.Bid{Bid: int32(bid), Name: name, Timestamp: curTime})
	if err != nil {
		mu.Lock()
		log.Println("Client", name, "bid attempt returned with error: \"", err.Error(), "\"at time", logicalTime)
		mu.Unlock()
		if len(nodeAddresses) <= 1 {
			fmt.Println("No nodes left")
			return
		}
		mu.Lock()
		nodeAddresses = nodeAddresses[:len(nodeAddresses)-1]
		log.Println("Client", name, "is attempting to make a connection to a new node at time", logicalTime)
		NewConnection()
		log.Println("Client", name, "is attempting to resend bid at time", logicalTime)
		mu.Unlock()
		Bid(bid)
	} else {
		mu.Lock()
		LogicalTime(bidAck.Timestamp)
		log.Println("Client", name, "has recieved bid acknowledgement with the outcome \"", bidAck.Outcome, "\"at time", logicalTime)
		mu.Unlock()
		fmt.Println(bidAck.Outcome)
	}
}

var nodeAddresses []string
var connection *grpc.ClientConn
var client pb.ClientClient
var logicalTime int32
var name string
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
