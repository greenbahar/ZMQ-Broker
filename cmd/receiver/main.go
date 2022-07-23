package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {
	context, _ := zmq.NewContext()

	socket, _ := context.NewSocket(zmq.REP)
	defer socket.Close()
	socket.Connect("tcp://localhost:5557")

	// check if there is some missed data to request from the broker
	lastMsgID := getLastMessageID()
	sync, _ := context.NewSocket(zmq.REQ)
	defer sync.Close()
	sync.Connect("tcp://*:5558")
	var reply string
	for reply != "finished" {
		sync.Send(strconv.Itoa(int(lastMsgID)), 0)
		reply, _ = sync.Recv(0)
		lastMsgID++
	}

	message := ""
	defer writeLastReceivedMessageToFile(message) // write "lastReceivedMessageID" value to the file

	for {
		message, _ = socket.Recv(0)
		log.Printf("Received: %s\n", message)
		socket.Send("ok", 0)
		fmt.Println("received message: ", message)
	}
}

func getLastMessageID() int64 {
	lastReceivedMessage, _ := os.ReadFile("./persistence/receiver/file.go")
	msg := strings.Split(string(lastReceivedMessage), " ")
	lastID, _ := strconv.ParseInt(msg[1], 10, 64)
	return lastID
}

func writeLastReceivedMessageToFile(msg string) {
	os.WriteFile("./persistence/receiver/file.go", []byte(msg), 0644)
}
