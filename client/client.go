package main

import (
	"net"
	"os"
	"strconv"

	pb "project-root/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	clientID, _ := strconv.Atoi(os.Args[1])
	clientPorts := []string{"50061", "50062", "50063"}

	listener, _ := net.Listen("tcp", "localhost:"+clientPorts[clientID])
	clientServe := grpc.NewServer()

	leaderAddress := "50053"
	connection, _ := grpc.NewClient("localhost:"+leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer connection.Close()

	client := pb.NewClientClient(connection)

	clientServe.Serve(listener)

}
