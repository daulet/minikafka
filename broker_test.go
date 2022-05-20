package minikafka_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daulet/minikafka"
)

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

	pub, err := minikafka.NewPublisher(
		minikafka.PublisherBrokerAddress(fmt.Sprintf("tcp://127.0.0.1:%d", pubPort)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	for i := 0; i < count; i++ {
		err = pub.Publish(ctx, "", []byte("Hello"))
		if err != nil {
			t.Fatalf("error publishing message: %v", err)
		}
	}

	cancel()
	wg.Wait()
}

func TestAllPublished(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	if err := os.Remove(fmt.Sprintf("%s/broker.log", os.TempDir())); err != nil {
		log.Println(err)
	}

	var (
		pubPort     = 5555
		subPort     = 5556
		expected    []string
		expectedMap = make(map[string]struct{})
	)
	for i := 0; i < 10; i++ {
		expected = append(expected, fmt.Sprintf("Hello %d", i))
		expectedMap[expected[i]] = struct{}{}
	}

	// run broker
	{
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
	}

	// multiple subscribers to broker
	<-time.After(100 * time.Millisecond)
	for i := 0; i < 3; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", subPort))
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()

		m := make(map[string]struct{})
		for k, s := range expectedMap {
			m[k] = s
		}

		wg.Add(1)
		go func(expectedMap map[string]struct{}) {
			defer wg.Done()
			bytes := make([]byte, 1024)

			bytes, err := io.ReadAll(bufio.NewReader(conn))
			if err != nil {
				log.Fatal(err)
			}

			raw := string(bytes)
			scnr := bufio.NewScanner(strings.NewReader(raw))
			for scnr.Scan() {
				msg := scnr.Text()
				if _, ok := expectedMap[msg]; !ok {
					t.Errorf("unexpected message received: %s", msg)
					continue
				}
				delete(expectedMap, msg)
			}

			for msg := range expectedMap {
				t.Errorf("expected message not received: %s", msg)
			}
		}(m)
	}

	pub, err := minikafka.NewPublisher(
		minikafka.PublisherBrokerAddress(fmt.Sprintf("tcp://127.0.0.1:%d", pubPort)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	for i := 0; i < len(expected); i++ {
		err = pub.Publish(ctx, "", []byte(expected[i]))
		if err != nil {
			t.Fatalf("error publishing message: %v", err)
		}
	}

	cancel()
	wg.Wait()
}
