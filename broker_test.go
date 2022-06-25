package minikafka_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/daulet/minikafka"
)

const brokerBootDelay = 10 * time.Millisecond

// TODO publisher should timeout on ack, broker won't send a nack in all cases
// TODO test for mix of succeeding and failing publishers, correct acks are received
func TestWritesAreAcked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	pubPort := 5555
	subPort := 5556
	count := 10
	topic := "test_topic"

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
	{
		// let the broker in goroutine chance to boot
		<-time.After(brokerBootDelay)
		pub, err := minikafka.NewPublisher(
			minikafka.PublisherBrokerAddress(fmt.Sprintf("127.0.0.1:%d", pubPort)),
			minikafka.PublisherTopic(topic),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer pub.Close()

		published := make(chan struct{}, count)
		for i := 0; i < count; i++ {
			go func() {
				err := pub.Publish(topic, []byte("Hello"))
				if err != nil {
					t.Errorf("error publishing message: %v", err)
				}
				published <- struct{}{}
			}()
		}
		for i := 0; i < count; i++ {
			<-published
		}
		close(published)
	}
	cancel()
	wg.Wait()
}

func TestAllPublished(t *testing.T) {
	var wg sync.WaitGroup
	subs := 3
	ctx, cancel := context.WithCancel(context.Background())
	topic := "test_topic"

	var (
		pubPort     = 5555
		subPort     = 5556
		expected    []string
		expectedMap = make(map[string]struct{})
	)
	for i := 0; i < 10; i++ {
		expected = append(expected, fmt.Sprintf("Hello\n%d", i))
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
				minikafka.SubscriberTopic(topic),
			)
			if err != nil {
				t.Error(err)
				return
			}
			defer sub.Close()

			for {
				bytes, err := sub.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					t.Error(err)
					return
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
	{
		pub, err := minikafka.NewPublisher(
			minikafka.PublisherBrokerAddress(fmt.Sprintf("127.0.0.1:%d", pubPort)),
			minikafka.PublisherTopic(topic),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer pub.Close()

		published := make(chan struct{}, len(expected))
		for i := 0; i < len(expected); i++ {
			go func(s string) {
				err := pub.Publish(topic, []byte(s))
				if err != nil {
					t.Errorf("error publishing message: %v", err)
				}
				published <- struct{}{}
			}(expected[i])
		}
		for i := 0; i < len(expected); i++ {
			<-published
		}
		close(published)
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
