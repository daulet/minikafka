package test

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/daulet/minikafka/client"
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())

	os.Exit(m.Run())
}

func TestWritesAreAcked(t *testing.T) {
	var (
		proxyPort = 9081
		proxyAddr = fmt.Sprintf("toxiproxy:%d", proxyPort)
		kafkaPort = 9091
		kafkaAddr = fmt.Sprintf("minikafka:%d", kafkaPort)

		topic = randSeq(10)
		msgs  = 10
	)
	// setup proxy behavior
	{
		client := toxiproxy.NewClient("toxiproxy:8474")
		proxy, err := client.CreateProxy("kafka_pub", proxyAddr, kafkaAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer proxy.Delete()
	}
	{
		pub, err := client.NewPublisher(
			client.PublisherBrokerAddress(proxyAddr),
			client.PublisherTopic(topic),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer pub.Close()

		published := make(chan struct{}, msgs)
		for i := 0; i < msgs; i++ {
			go func() {
				payload := []byte("Hello")
				err := pub.Publish(topic, &payload)
				if err != nil {
					t.Errorf("error publishing message: %v", err)
				}
				published <- struct{}{}
			}()
		}
		for i := 0; i < msgs; i++ {
			<-published
		}
		close(published)
	}
}

func TestPacketPartitioning(t *testing.T) {
	var (
		proxyPubPort = 9081
		proxySubPort = 9082
		kafkaPubPort = 9091
		kafkaSubPort = 9092

		topic = randSeq(10)
		subs  = 3
	)
	// setup proxy behavior
	{
		client := toxiproxy.NewClient("toxiproxy:8474")
		{
			proxy, err := client.CreateProxy("kafka_pub",
				fmt.Sprintf("toxiproxy:%d", proxyPubPort),
				fmt.Sprintf("minikafka:%d", kafkaPubPort),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer proxy.Delete()
			proxy.AddToxic("small_packets", "slicer", "upstream", 1.0, toxiproxy.Attributes{
				"average_size":   1, // bytes
				"size_variation": 1,
				"delay":          100, // microseconds
			})
		}
		{
			proxy, err := client.CreateProxy("kafka_sub",
				fmt.Sprintf("toxiproxy:%d", proxySubPort),
				fmt.Sprintf("minikafka:%d", kafkaSubPort),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer proxy.Delete()
			proxy.AddToxic("small_packets", "slicer", "upstream", 1.0, toxiproxy.Attributes{
				"average_size":   1, // bytes
				"size_variation": 1,
				"delay":          100, // microseconds
			})
		}
	}

	expected := make(map[string]struct{})
	for i := 0; i < 10; i++ {
		expected[fmt.Sprintf("Hello\n%d", i)] = struct{}{}
	}

	// publish messages
	{
		pub, err := client.NewPublisher(
			client.PublisherBrokerAddress(fmt.Sprintf("toxiproxy:%d", proxyPubPort)),
			client.PublisherTopic(topic),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer pub.Close()

		published := make(chan struct{}, len(expected))
		for msg := range expected {
			go func(s string) {
				payload := []byte(s)
				err := pub.Publish(topic, &payload)
				if err != nil {
					t.Errorf("error publishing message: %v", err)
				}
				published <- struct{}{}
			}(msg)
		}
		for range expected {
			<-published
		}
		close(published)
	}
	// multiple subscribers on the same topic
	{
		subCh := make(chan struct{}, subs)
		for i := 0; i < subs; i++ {
			exp := make(map[string]struct{})
			for k, s := range expected {
				exp[k] = s
			}

			go func(expected map[string]struct{}) {
				sub, err := client.NewSubscriber(
					client.SubscriberBrokerAddress(fmt.Sprintf("toxiproxy:%d", proxySubPort)),
					client.SubscriberTopic(topic),
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
					if _, ok := expected[msg]; !ok {
						t.Errorf("unexpected message received: %s", msg)
						continue
					}
					delete(expected, msg)
					if len(expected) == 0 {
						break
					}
				}

				for msg := range expected {
					t.Errorf("expected message not received: %s", msg)
				}
				subCh <- struct{}{}
			}(exp)
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
	}
}

func TestAllPublished(t *testing.T) {
	var (
		proxyPubPort = 9081
		proxySubPort = 9082
		kafkaPubPort = 9091
		kafkaSubPort = 9092

		topic = randSeq(10)
		subs  = 3
	)
	// setup proxy behavior
	{
		client := toxiproxy.NewClient("toxiproxy:8474")
		{
			proxy, err := client.CreateProxy("kafka_pub",
				fmt.Sprintf("toxiproxy:%d", proxyPubPort),
				fmt.Sprintf("minikafka:%d", kafkaPubPort),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer proxy.Delete()
		}
		{
			proxy, err := client.CreateProxy("kafka_sub",
				fmt.Sprintf("toxiproxy:%d", proxySubPort),
				fmt.Sprintf("minikafka:%d", kafkaSubPort),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer proxy.Delete()
		}
	}

	expected := make(map[string]struct{})
	for i := 0; i < 10; i++ {
		expected[fmt.Sprintf("Hello\n%d", i)] = struct{}{}
	}

	// publish messages
	{
		pub, err := client.NewPublisher(
			client.PublisherBrokerAddress(fmt.Sprintf("toxiproxy:%d", proxyPubPort)),
			client.PublisherTopic(topic),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer pub.Close()

		published := make(chan struct{}, len(expected))
		for msg := range expected {
			go func(s string) {
				payload := []byte(s)
				err := pub.Publish(topic, &payload)
				if err != nil {
					t.Errorf("error publishing message: %v", err)
				}
				published <- struct{}{}
			}(msg)
		}
		for range expected {
			<-published
		}
		close(published)
	}
	// multiple subscribers on the same topic
	{
		subCh := make(chan struct{}, subs)
		for i := 0; i < subs; i++ {
			exp := make(map[string]struct{})
			for k, s := range expected {
				exp[k] = s
			}

			go func(expected map[string]struct{}) {
				sub, err := client.NewSubscriber(
					client.SubscriberBrokerAddress(fmt.Sprintf("toxiproxy:%d", proxySubPort)),
					client.SubscriberTopic(topic),
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
					if _, ok := expected[msg]; !ok {
						t.Errorf("unexpected message received: %s", msg)
						continue
					}
					delete(expected, msg)
					if len(expected) == 0 {
						break
					}
				}

				for msg := range expected {
					t.Errorf("expected message not received: %s", msg)
				}
				subCh <- struct{}{}
			}(exp)
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
	}
}
