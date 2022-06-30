package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daulet/minikafka"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cancel()
		os.Exit(1)
	}()

	pubPort := 9091
	subPort := 9092
	storeDir := "/data"
	broker := minikafka.NewBroker(
		minikafka.BrokerPublishPort(pubPort),
		minikafka.BrokerSubscribePort(subPort),
		minikafka.BrokerStoreaDir(storeDir),
		minikafka.BrokerPollingTimeout(time.Second),
	)
	broker.Run(ctx)
}
