package minikafka_bench

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/daulet/minikafka"
)

func BenchmarkPublish1Topic(b *testing.B) {
	var (
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
		msgs        = b.N

		pubPort = 5555
		subPort = 5556
		topic   = "test_topic"
	)

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
		pub, err := minikafka.NewPublisher(
			minikafka.PublisherBrokerAddress(fmt.Sprintf("127.0.0.1:%d", pubPort)),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer pub.Close()

		b.ResetTimer()

		published := make(chan struct{}, msgs)
		for i := 0; i < msgs; i++ {
			go func() {
				err := pub.Publish(topic, []byte("Hello"))
				if err != nil {
					b.Errorf("error publishing message: %v", err)
				}
				published <- struct{}{}
			}()
		}
		for i := 0; i < msgs; i++ {
			<-published
		}
		close(published)

		b.StopTimer()
	}
	cancel()
	wg.Wait()
}
