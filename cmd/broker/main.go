/*
	broker-storage is used not to lose messages in case the sender or the receiver-storage shut down.
	The broker-storage stores data too
	store data of size 50-8000 bytes of minimum rate 10000 message per second. And
*/

package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	broker_storage "windows-old/interview/graph-broker/persistence/file/broker-storage"
)

var (
	inMemoryStorage          = make(map[int64]string)
	NumberOfSentMessages     = 0
	NumberOfReceivedMessages = 0
	mu                       = sync.Mutex{}
)

func main() {
	context, _ := zmq.NewContext()

	// communicate to sender module (to "receive" data from sender module)
	receiver, _ := context.NewSocket(zmq.REP)
	receiver.Bind("tcp://*:5556")
	defer receiver.Close()

	// communicate to receiver-storage module (to "send" data to receiver-storage module)
	sender, _ := context.NewSocket(zmq.REQ)
	defer sender.Close()
	sender.Bind("tcp://*:5557")

	StoreDataToFile()

	logNumberOfTransferredMessages()

	// communicate to sync part of the receiver-storage
	needInterrupt := false
	go func() {
		sync, _ := context.NewSocket(zmq.REP)
		receiver.Bind("tcp://*:5558")
		defer receiver.Close()

		isIdInStoredMessages := true
		fileStorage, err := loadDataFromFileStorage()
		if err != nil {
			panic(err)
		}
		for isIdInStoredMessages {
			Id, _ := sync.Recv(0)
			needInterrupt = true
			ID, _ := strconv.ParseInt(Id, 10, 64)
			if msg, ok := fileStorage[ID]; ok {
				sync.Send(msg, 0)
				NumberOfReceivedMessages++
			} else {
				sync.Send("finished", 0)
				isIdInStoredMessages = false
				needInterrupt = false
			}
		}
	}()

	var totalReceivedInBroker, totalSent int64

	i := 0
	for {
		if needInterrupt {
			time.Sleep(time.Second)
		}

		rawMessage, rcvErr := receiver.Recv(0)
		if rcvErr == nil {
			i++
			totalReceivedInBroker++
			receiver.Send("received", 0)
			NumberOfSentMessages++
			fmt.Println(".........")

			sender.Send(rawMessage, 0) // ok <nil>
			_, err := sender.Recv(0)   // 5717 <nil>
			if err == nil {
				NumberOfReceivedMessages++
				totalSent++
				continue
			}

			data := strings.Split(rawMessage, " ")
			messageID, _ := strconv.ParseInt(data[1], 10, 64)
			message := data[2]
			mu.Lock()
			inMemoryStorage[messageID] = message
			mu.Unlock()
		}
	}
}

func loadDataFromFileStorage() (map[int64]string, error) {
	return broker_storage.ReadFileContent("./persistence/file/broker-storage/fileStorage.txt")
}

func StoreDataToFile() {
	// Persist data to file every 2 seconds for example
	ticker := time.NewTicker(2 * time.Second)
	brokerStorage := broker_storage.NewFileStorage()

	go func() {
		for {
			select {
			case <-ticker.C:
				mu.Lock()
				fmt.Println(inMemoryStorage)
				err := brokerStorage.AppendToFile("./persistence/file/broker-storage/fileStorage.txt", inMemoryStorage)
				mu.Unlock()
				if err != nil {
					log.Println(err)
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ticker.C:
				mu.Lock()
				err := brokerStorage.AppendToFile("./persistence/file/broker-storage/fileStorage.txt", inMemoryStorage)
				mu.Unlock()
				if err != nil {
					log.Println(err)
				}
			}
		}
	}()
}

func logNumberOfTransferredMessages() {
	go func() {
		for {
			time.Sleep(time.Second)
			mu.Lock()
			fmt.Println("number of sent messages: ", NumberOfSentMessages, "******", "number of received messages: ", NumberOfReceivedMessages)
			mu.Unlock()
		}
	}()
}
