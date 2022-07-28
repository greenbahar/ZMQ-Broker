package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	NumberOfReceivedMessages = 0
	message                  = ""
)

func main() {
	context, _ := zmq.NewContext()

	socket, _ := context.NewSocket(zmq.REP)
	defer socket.Close()
	socket.Connect("tcp://localhost:5557")

	// check if there is some missed data to request from the broker-storage
	lastMsgID := getLastMessageID()
	fmt.Println(lastMsgID)
	if lastMsgID != 0 {
		sync, _ := context.NewSocket(zmq.REQ)
		defer sync.Close()
		sync.Connect("tcp://*:5558")
		var reply string
		for reply != "finished" {
			fmt.Println("synced...")
			sync.Send(strconv.Itoa(int(lastMsgID)), 0)
			reply, _ = sync.Recv(0)
			lastMsgID++
		}
	}
	fmt.Println("sync finished")

	writeLastReceivedMessageToFile() // write "lastReceivedMessageID message" value to the file

	logNumberOfReceivedMessages()
	for {
		message, _ = socket.Recv(0)
		//log.Printf("Received: %s\n", message)
		socket.Send("ok", 0)
		NumberOfReceivedMessages++
		//fmt.Println("received message: ", message)
	}
}

func getLastMessageID() int64 {
	lastReceivedMessage, _ := os.ReadFile("./persistence/file/receiver-storage/fileStorage.txt")
	msg := strings.Split(string(lastReceivedMessage), " ")
	if len(msg) > 1 {
		lastID, _ := strconv.ParseInt(msg[1], 10, 64)
		//fmt.Println("lastReceivedMessage: ", lastReceivedMessage)
		return lastID
	}

	return 0
}

func writeLastReceivedMessageToFile() {
	ticker := time.NewTicker(1 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				err := os.WriteFile("./persistence/file/receiver-storage/fileStorage.txt", []byte(message), 0700)
				if err != nil {
					log.Println(err)
				}
			}
		}
	}()
}

func logNumberOfReceivedMessages() {
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Println("number of received messages: ", NumberOfReceivedMessages)
		}
	}()
}
