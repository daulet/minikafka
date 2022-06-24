package test

import (
	"fmt"
	"testing"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/daulet/minikafka"
)

func TestWritesAreAcked(t *testing.T) {
	var (
		proxyPort = 9081
		kafkaPort = 9091
	)
	client := toxiproxy.NewClient("toxiproxy:8474")
	proxy, err := client.CreateProxy("kafka",
		fmt.Sprintf("toxiproxy:%d", proxyPort),
		fmt.Sprintf("minikafka:%d", kafkaPort),
	)
	if err != nil {
		panic(err)
	}
	defer proxy.Delete()

	{
		count := 10

		pub, err := minikafka.NewPublisher(
			minikafka.PublisherBrokerAddress(fmt.Sprintf("toxiproxy:%d", proxyPort)),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer pub.Close()

		published := make(chan struct{}, count)
		for i := 0; i < count; i++ {
			go func() {
				err := pub.Publish("", []byte("Hello"))
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
}
