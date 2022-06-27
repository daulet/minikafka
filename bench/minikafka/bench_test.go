package bench_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/daulet/minikafka"
	"github.com/daulet/minikafka/client"
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
		pub, err := client.NewPublisher(
			client.PublisherBrokerAddress(fmt.Sprintf("127.0.0.1:%d", pubPort)),
			client.PublisherTopic(topic),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer pub.Close()

		b.ResetTimer()

		var (
			workGrp sync.WaitGroup
			workers = 4 * 1024
			payload = []byte("Hello")
		)
		workGrp.Add(workers)
		for i := 0; i < workers; i++ {
			count := msgs / workers
			if i < msgs%workers {
				count++
			}
			go func(count int) {
				defer workGrp.Done()
				for i := 0; i < count; i++ {
					if err := pub.Publish(topic, payload); err != nil {
						b.Errorf("error publishing message: %v", err)
					}
				}
			}(count)
		}
		workGrp.Wait()

		b.StopTimer()
	}
	cancel()
	wg.Wait()
}

func Test_Publish1Topic(t *testing.T) {
	bm := testing.Benchmark(BenchmarkPublish1Topic)
	fmt.Printf("BenchmarkPublish1Topic:		%v		%v ns/op\n", bm.N, bm.NsPerOp())
	if bm.NsPerOp() > 11000 { // 11 microseconds, based on Github Actions SKU
		panic(fmt.Errorf("BenchmarkPublish1Topic speed is too low: %v ns/op", bm.NsPerOp()))
	}
}
