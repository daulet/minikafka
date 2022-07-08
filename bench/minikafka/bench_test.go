package bench_test

import (
	bench "bench/minikafka"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/daulet/minikafka"
	"github.com/daulet/minikafka/client"
	"github.com/jamiealquiza/tachymeter"
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

func BenchmarkEndToEndLatency(b *testing.B) {
	var (
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
		total       = int64(b.N)

		pubPort = 5555
		subPort = 5556
		subs    = 1
		topic   = randSeq(10)
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

	// multiple subscribers to broker
	subP99 := make(chan time.Duration, subs)
	for i := 0; i < subs; i++ {
		go func(count int64) {
			sub, err := client.NewSubscriber(
				client.SubscriberBrokerAddress(fmt.Sprintf("127.0.0.1:%d", subPort)),
				client.SubscriberTopic(topic),
			)
			if err != nil {
				b.Error(err)
				return
			}
			defer sub.Close()

			t := tachymeter.New(&tachymeter.Config{Size: 100000})
			for {
				bytes, err := sub.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					b.Error(err)
					return
				}

				sent := int64(binary.LittleEndian.Uint64(bytes))
				latency := time.Since(time.Unix(0, sent))
				t.AddTime(latency)

				count--
				if count == 0 {
					break
				}
			}
			subP99 <- t.Calc().Time.P99
		}(total)
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

		for i := int64(0); i < total; i++ {
			ts := make([]byte, 8)
			binary.LittleEndian.PutUint64(ts, uint64(time.Now().UnixNano()))
			select {
			case msgCh <- &ts:
				continue
			default:
			}

			workGrp.Add(1)
			go func() {
				defer workGrp.Done()

				for ts := range msgCh {
					err := pub.Publish(topic, ts)
					if err != nil {
						b.Errorf("error publishing message: %v", err)
					}
				}
			}()

			msgCh <- &ts
		}
		close(msgCh)
		workGrp.Wait()
	}
	// subscribers exit when all expected messages are received
	worstP99 := time.Duration(0)
	for i := 0; i < subs; i++ {
		p99 := <-subP99
		if p99 > worstP99 {
			worstP99 = p99
		}
	}
	b.StopTimer()

	cancel()
	wg.Wait()

	b.ReportMetric(float64(worstP99), "ns(p99)")
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("------------------------------------------------------")
	fmt.Printf("NumCPU=%v\n", runtime.NumCPU())
	fmt.Printf("GOMAXPROCS=%v\n", runtime.GOMAXPROCS(0))

	if out, err := exec.Command("lscpu").Output(); err != nil {
		panic(err)
	} else {
		fmt.Printf("\n%s\n", out)
	}
	fmt.Println("------------------------------------------------------")

	os.Exit(m.Run())
}

func Test_Publish1Topic(t *testing.T) {
	bm := testing.Benchmark(BenchmarkPublish1Topic)
	fmt.Printf("BenchmarkPublish1Topic:		%v		%v ns/op\n", bm.N, bm.NsPerOp())
	if bm.NsPerOp() > 14000 { // 14 microseconds, based on Github Actions SKU
		panic(fmt.Errorf("BenchmarkPublish1Topic speed is too low: %v ns/op", bm.NsPerOp()))
	}
}

func Test_Throughput(t *testing.T) {
	bm := testing.Benchmark(BenchmarkThroughput)
	fmt.Printf("BenchmarkThroughput:		%v		%v ns/op\n", bm.N, bm.NsPerOp())
	if bm.NsPerOp() > 16000 { // 16 microseconds, based on Github Actions SKU
		panic(fmt.Errorf("BenchmarkThroughput speed is too low: %v ns/op", bm.NsPerOp()))
	}
}

func Test_Latency(t *testing.T) {
	bm := testing.Benchmark(BenchmarkEndToEndLatency)
	p99 := time.Duration(bm.Extra["ns(p99)"] * float64(time.Nanosecond))
	fmt.Printf("BenchmarkEndToEndLatency (p99):		%v		%v\n", bm.N, p99)
	if p99 > time.Duration(1.5*float64(time.Second)) { // 1.5 sec, based on Github Actions SKU
		panic(fmt.Errorf("BenchmarkEndToEndLatency (p99) is too high: %v", p99))
	}
}

// Test idle consumption while some subscribers are connected
func Test_BenchmarkIdleTimeServingSubscribers(t *testing.T) {
	var (
		wg          sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())

		pubPort = 5555
		subPort = 5556
		pubs    = 10
		subs    = 10
		msgs    = 10
		topic   = randSeq(10)
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

	var (
		subCtx, subCancel = context.WithCancel(context.Background())
		subGrp            sync.WaitGroup
	)
	{
		subGrp.Add(subs)
		for i := 0; i < subs; i++ {
			go func(ctx context.Context, _ int) {
				defer subGrp.Done()

				sub, err := client.NewSubscriber(
					client.SubscriberBrokerAddress(fmt.Sprintf("127.0.0.1:%d", subPort)),
					client.SubscriberTopic(topic),
				)
				if err != nil {
					t.Error(err)
					return
				}
				defer sub.Close()

				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					_, err := sub.Read()
					if err != nil {
						if err, ok := err.(net.Error); ok && err.Timeout() {
							continue
						}
						t.Error(err)
						break
					}
				}
			}(subCtx, msgs)
		}
	}
	var pubGrp sync.WaitGroup
	{
		pubGrp.Add(pubs)
		for i := 0; i < pubs; i++ {
			go func() {
				defer pubGrp.Done()

				pub, err := client.NewPublisher(
					client.PublisherBrokerAddress(fmt.Sprintf("127.0.0.1:%d", pubPort)),
					client.PublisherTopic(topic),
				)
				if err != nil {
					t.Error(err)
					return
				}
				defer pub.Close()

				for i := 0; i < msgs; i++ {
					payload := []byte("test message")
					if err := pub.Publish(topic, &payload); err != nil {
						t.Errorf("error publishing message: %v", err)
					}
				}
			}()
		}
	}

	// make sure publishers don't polute metrics
	pubGrp.Wait()

	// measure idle time with some subscribers

	cpuBefore := bench.GeProcessCpuTime()
	<-time.After(time.Second)
	cpuAfter := bench.GeProcessCpuTime()
	cputime := cpuAfter - cpuBefore
	t.Logf("Idle CPU time = %s\n", cputime)

	// before sleeping in broker.publish() CPU time was over 10s for 1s walltime
	if cputime > time.Duration(50*time.Millisecond) {
		t.Errorf("Idle CPU time is too high: %v", cputime)
	}

	// shutdown subscribers
	subCancel()
	subGrp.Wait()

	cancel()
	wg.Wait()
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
