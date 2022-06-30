package bench_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
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
			msgCh   = make(chan *[]byte)
			payload = []byte("Hello")
		)

		for i := 0; i < msgs; i++ {
			select {
			case msgCh <- &payload:
				continue
			default:
			}

			workGrp.Add(1)
			go func() {
				defer workGrp.Done()

				for msg := range msgCh {
					if err := pub.Publish(topic, msg); err != nil {
						b.Errorf("error publishing message: %v", err)
					}
				}
			}()

			msgCh <- &payload
		}
		close(msgCh)
		workGrp.Wait()

		b.StopTimer()
	}
	cancel()
	wg.Wait()
}

func BenchmarkThroughput(b *testing.B) {
	var (
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
		msgs        = b.N

		pubPort = 5555
		subPort = 5556
		subs    = 1
		topic   = randSeq(10)

		expected = make(map[string]struct{})
	)
	for i := 0; i < msgs; i++ {
		expected[fmt.Sprintf("Hello\n%d", i)] = struct{}{}
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
	subCh := make(chan struct{}, subs)
	for i := 0; i < subs; i++ {
		exp := make(map[string]struct{})
		for k, s := range expected {
			exp[k] = s
		}

		go func(expected map[string]struct{}) {
			defer func() {
				subCh <- struct{}{}
			}()

			sub, err := client.NewSubscriber(
				client.SubscriberBrokerAddress(fmt.Sprintf("127.0.0.1:%d", subPort)),
				client.SubscriberTopic(topic),
			)
			if err != nil {
				b.Error(err)
				return
			}
			defer sub.Close()

			for {
				bytes, err := sub.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					b.Error(err)
					return
				}
				msg := string(bytes)
				if _, ok := expected[msg]; !ok {
					b.Errorf("unexpected message received: %s", msg)
					continue
				}
				delete(expected, msg)
				if len(expected) == 0 {
					break
				}
			}

			for msg := range expected {
				b.Errorf("expected message not received: %s", msg)
			}
		}(exp)
	}
	{
		pub, err := client.NewPublisher(
			client.PublisherBrokerAddress(fmt.Sprintf("127.0.0.1:%d", pubPort)),
			client.PublisherTopic(topic),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer pub.Close()

		var (
			workGrp sync.WaitGroup
			msgCh   = make(chan *[]byte)
		)
		b.ResetTimer()

		for msg := range expected {
			payload := []byte(msg)
			select {
			case msgCh <- &payload:
				continue
			default:
			}

			workGrp.Add(1)
			go func() {
				defer workGrp.Done()

				for msg := range msgCh {
					err := pub.Publish(topic, msg)
					if err != nil {
						b.Errorf("error publishing message: %v", err)
					}
				}
			}()

			msgCh <- &payload
		}
		close(msgCh)
		workGrp.Wait()
	}
	// subscribers exit when all expected messages are received
	for i := 0; i < subs; i++ {
		<-subCh
	}
	b.StopTimer()

	cancel()
	wg.Wait()
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())

	os.Exit(m.Run())
}

func Test_Publish1Topic(t *testing.T) {
	bm := testing.Benchmark(BenchmarkPublish1Topic)
	fmt.Printf("BenchmarkPublish1Topic:		%v		%v ns/op\n", bm.N, bm.NsPerOp())
	if bm.NsPerOp() > 14000 { // 14 microseconds, based on Github Actions SKU
		panic(fmt.Errorf("BenchmarkPublish1Topic speed is too low: %v ns/op", bm.NsPerOp()))
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
