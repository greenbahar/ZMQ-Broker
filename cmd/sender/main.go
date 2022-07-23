/*
	sender:
		the server do not need to get the reply from the broker so no need to REQ-RES pattern
		there is no need to parallel workers (brokers) so no need to use PUSH-PULL pattern
		as far as the tasks demands; the server needs to send requests (of rate more than 10kbps) to the broker. Hence:
			- server would have a goroutine to calculate the messages per second and if it was less, the n the server
			  would create another goroutine to start another instance of the message publisher to work concurrently to
			  reach the desired mps. the server starts with one instance and adds or removes more instances when needed.
		the messages need to have the id to calculate number of received messages in the receiver. Hence:
			- server needs to publish a message to send the total messages to the broker (to be sent to the receiver)
*/

package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"math/rand"
	"os"
	"time"
)

func main() {
	context, _ := zmq.NewContext()
	socket, _ := context.NewSocket(zmq.REQ)
	defer socket.Close()
	if err := socket.Connect("tcp://localhost:5556"); err != nil {
		panic(err)
	}

	random8kFile, err := os.ReadFile("utils/randomGenerator/random8kFile.txt")
	if err != nil {
		panic(err)
	}

	counter := 0
	ticker := time.NewTicker(time.Second)

	// get the rate (msg per second) of sending messages and create another instance (goroutine) if needed
	//var w *sync.WaitGroup
	go func() {
		//w.Add(1)
		//defer w.Done()
		for {
			select {
			case <-ticker.C:
				fmt.Println(counter)
				counter = 0
			}
		}
	}()

	id := 0
	for {
		messageSize := rand.Intn(8000) + 50
		message := fmt.Sprintf("id: %d %s", id, string(random8kFile[:messageSize]))
		socket.Send(message, 0)
		fmt.Println(id, message)
		reply, _ := socket.Recv(0)
		fmt.Println(reply)
		counter++
		id++
	}

	//w.Wait()
}
