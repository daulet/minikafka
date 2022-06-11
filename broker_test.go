package minikafka_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/daulet/minikafka"
)

const brokerBootDelay = 10 * time.Millisecond

// TODO publisher should timeout on ack, broker won't send a nack in all cases
// TODO if publisher is concurrent, how does it distinguish acks from different goroutines? - update test accordingly
// TODO complement to above: multiple publishers, some failing, correct acks are received
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

	// let the broker in goroutine chance to boot
	<-time.After(brokerBootDelay)
	pub, err := minikafka.NewPublisher(
		minikafka.PublisherBrokerAddress(fmt.Sprintf("127.0.0.1:%d", pubPort)),
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
	subs := 3
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

	// let the broker in goroutine chance to boot
	<-time.After(brokerBootDelay)
	// multiple subscribers to broker
	subCh := make(chan struct{}, subs)
	for i := 0; i < subs; i++ {
		m := make(map[string]struct{})
		for k, s := range expectedMap {
			m[k] = s
		}

		go func(expectedMap map[string]struct{}) {
			sub, err := minikafka.NewSubscriber(
				minikafka.SubscriberBrokerAddress(fmt.Sprintf("127.0.0.1:%d", subPort)),
			)
			if err != nil {
				log.Fatal(err)
			}
			defer sub.Close()

			for {
				bytes, err := sub.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Fatal(err)
				}
				msg := string(bytes)
				if _, ok := expectedMap[msg]; !ok {
					t.Errorf("unexpected message received: %s", msg)
					continue
				}
				delete(expectedMap, msg)
				if len(expectedMap) == 0 {
					break
				}
			}

			for msg := range expectedMap {
				t.Errorf("expected message not received: %s", msg)
			}

			subCh <- struct{}{}
		}(m)
	}

	pub, err := minikafka.NewPublisher(
		minikafka.PublisherBrokerAddress(fmt.Sprintf("127.0.0.1:%d", pubPort)),
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
	// subscribers exit when all expected messages are received
	for i := 0; i < subs; i++ {
		select {
		case <-subCh:
			continue
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for subscriber to receive all expected messages")
		}
	}

	cancel()
	wg.Wait()
}
