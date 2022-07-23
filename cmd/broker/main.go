/*
	broker is used not to lose messages in case the sender or the receiver shut down.
	The broker stores data too
	store data of size 50-8000 bytes of minimum rate 10000 message per second. And
*/

package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"strconv"
	"strings"
	"time"
)

func main() {
	context, _ := zmq.NewContext()

	// communicate to sender module (to "receive" data from sender module)
	receiver, _ := context.NewSocket(zmq.REP)
	receiver.Bind("tcp://*:5556")
	defer receiver.Close()

	// communicate to receiver module (to "send" data to receiver module)
	sender, _ := context.NewSocket(zmq.REQ)
	defer sender.Close()
	sender.Bind("tcp://*:5557")

	// communicate to sync part of the receiver
	//signalToInterruptReceivingFromSenderServer := make(chan bool)
	needInterrupt := false
	go func() {
		//signalToInterruptReceivingFromSenderServer <- true

		sync, _ := context.NewSocket(zmq.REP)
		receiver.Bind("tcp://*:5558")
		defer receiver.Close()
		isIdInStoredMessages := true
		fileStorage := loadDataFromFileStorage()
		for isIdInStoredMessages {
			Id, _ := sync.Recv(0)
			needInterrupt = true
			ID, _ := strconv.ParseInt(Id, 10, 64)
			if msg, ok := fileStorage[ID]; ok {
				sync.Send(msg, 0)
			} else {
				sync.Send("finished", 0)
				isIdInStoredMessages = false
				//signalToInterruptReceivingFromSenderServer <- false
				needInterrupt = false
			}
		}
	}()

	var totalReceivedInBroker, totalSent int64
	storage := make(map[int64]string)
	//ticker := time.NewTicker(5*time.Second)
	//ack := make(chan int,10000)
	//nack := make(chan bool, 1)
	//
	//go func() {
	//	for {
	//		select {
	//		case <-ticker.C:
	//			nack <- true
	//		case <- ack:
	//			fmt.Println("still live...")
	//			continue
	//		}
	//	}
	//}()

	i := 0
	for {
		if i%1000 == 0 { // For my own tests
			fmt.Println("done", i)
		}

		if needInterrupt {
			time.Sleep(time.Second)
		}

		rawMessage, rcvErr := receiver.Recv(0)
		if rcvErr == nil {
			i++
			totalReceivedInBroker++
			fmt.Println("ack")
			receiver.Send("received", 0)

			sender.Send(rawMessage, 0)
			_, rcvERR := sender.Recv(0)
			if rcvERR != nil {
				data := strings.Split(rawMessage, " ")
				messageID, _ := strconv.ParseInt(data[1], 10, 64)
				message := data[2]
				storage[messageID] = message
			} else {
				totalSent++
			}
		}
	}

	//fmt.Printf("number of received messages is %d and total number of sent messages is %d", totalReceived, totalSent)
}

func loadDataFromFileStorage() map[int64]string {
	// TODO
	return nil
}
