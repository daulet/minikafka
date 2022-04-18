package minikafka_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/daulet/minikafka"
	"github.com/zeromq/goczmq"
)

// TODO implement publisher that abstracts away zmq and wait on receive to ack
// TODO publisher should timeout on ack, broker won't send a nack in all cases
// TODO if publisher is concurrent, how does it distinguish acks from different goroutines? - update test accordingly
func TestWritesAreAcked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	pubPort := 5555
	subPort := 5556
	count := 10

	broker := minikafka.NewBroker(
		minikafka.BrokerPublishPort(pubPort),
		minikafka.BrokerSubscribePort(subPort),
		minikafka.BrokerStoreaDir(os.TempDir()),
		minikafka.BrokerPollingTimeout(100*time.Millisecond),
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		broker.Run(ctx)
	}()

	dealer, err := goczmq.NewDealer(fmt.Sprintf("tcp://127.0.0.1:%d", pubPort))
	if err != nil {
		log.Fatal(err)
	}
	defer dealer.Destroy()

	for i := 0; i < count; i++ {
		err = dealer.SendFrame([]byte("Hello"), goczmq.FlagNone)
		if err != nil {
			t.Fatalf("error sending frame: %v", err)
		}
		// wait for ack, there is no way to distinguish between acks for different messages
		reply, err := dealer.RecvMessage()
		if err != nil {
			log.Fatal(err)
		}
		if string(reply[0]) != "OK" {
			t.Fatalf("expected OK, got %s", reply[0])
		}
	}

	cancel()
	wg.Wait()
}
