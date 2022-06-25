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
)

func TestFailedSubscriber(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	var (
		pubPort      = 5555
		subPort      = 5556
		messageCount = 10
		topic        = "test_topic"
	)

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
	// whether subscriber is done
	subCh := make(chan struct{}, 1)
	go func() {
		sub, err := minikafka.NewSubscriber(
			minikafka.SubscriberBrokerAddress(fmt.Sprintf("127.0.0.1:%d", subPort)),
			minikafka.SubscriberTopic(topic),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer sub.Close()

		// this test is about not reading all messages and quiting
		for i := 0; i < messageCount/2; i++ {
			_, err := sub.Read()
			if err != nil {
				// not expecting to reach EOF
				log.Fatal(err)
			}
		}

		subCh <- struct{}{}
	}()
	{
		pub, err := minikafka.NewPublisher(
			minikafka.PublisherBrokerAddress(fmt.Sprintf("127.0.0.1:%d", pubPort)),
			minikafka.PublisherTopic(topic),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer pub.Close()

		published := make(chan struct{}, messageCount)
		for i := 0; i < messageCount; i++ {
			go func(i int) {
				err := pub.Publish(topic, []byte(fmt.Sprintf("Hello %d", i)))
				if err != nil {
					t.Errorf("error publishing message: %v", err)
				}
				published <- struct{}{}
			}(i)
		}
		for i := 0; i < messageCount; i++ {
			<-published
		}
		close(published)
	}
	select {
	case <-subCh:
		break
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subscriber to exit")
	}

	cancel()
	wg.Wait()
}
