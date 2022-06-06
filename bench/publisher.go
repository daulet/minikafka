package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <address>\n", os.Args[0])
		os.Exit(1)
	}

	var (
		ctx = context.Background()
		wg  sync.WaitGroup
	)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": os.Args[1]})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// Delivery report handler for produced messages
	wg.Add(1)
	subctx, cancel := context.WithCancel(ctx)
	go func() {
		defer wg.Done()
		done := false
		for !done {
			select {
			case e := <-p.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
					} else {
						fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					}
				default:
					fmt.Printf("Ignored event: %v\n", ev)
				}
			case <-subctx.Done():
				done = true
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "test"
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(word),
		}, nil)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * int(time.Millisecond))
	cancel()
	wg.Wait()
}
