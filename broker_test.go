package minikafka_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/daulet/minikafka"
	"github.com/zeromq/goczmq"
)

func TestWritesAreAcked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	port := 5555
	count := 10

	broker := minikafka.NewBroker(
		minikafka.BrokerPort(port),
		minikafka.BrokerStoreaDir(os.TempDir()),
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		broker.Run(ctx)
	}()

	dealer, err := goczmq.NewDealer(fmt.Sprintf("tcp://127.0.0.1:%d", port))
	if err != nil {
		log.Fatal(err)
	}
	defer dealer.Destroy()

	for i := 0; i < count; i++ {
		err = dealer.SendFrame([]byte("Hello"), goczmq.FlagNone)
		if err != nil {
			t.Fatalf("error sending frame: %v", err)
		}
	}

	for i := 0; i < count; i++ {
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
