package minikafka

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/zeromq/goczmq"
)

/*
	Broker process is to be booted on each node of deployment.
*/
type Broker struct {
	port           int
	storageDir     string
	pollingTimeout time.Duration
}

type BrokerConfig func(b *Broker)

func BrokerPort(port int) BrokerConfig {
	return func(b *Broker) {
		b.port = port
	}
}

func BrokerStoreaDir(dir string) BrokerConfig {
	return func(b *Broker) {
		b.storageDir = dir
	}
}

func BrokerPollingTimeout(timeout time.Duration) BrokerConfig {
	return func(b *Broker) {
		b.pollingTimeout = timeout
	}
}

func NewBroker(opts ...BrokerConfig) *Broker {
	b := &Broker{}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *Broker) Run(ctx context.Context) {

	router, err := goczmq.NewRouter(fmt.Sprintf("tcp://*:%d", b.port))
	if err != nil {
		log.Fatal(err)
	}
	defer router.Destroy()

	poller, err := goczmq.NewPoller(router)
	if err != nil {
		log.Fatal(err)
	}
	defer poller.Destroy()

	f, err := os.OpenFile(fmt.Sprintf("%s/broker.log", b.storageDir), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		socket := poller.Wait(int(b.pollingTimeout / time.Millisecond))
		if socket == nil {
			continue
		}

		reqData, err := socket.RecvMessage()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("received %v from %v", reqData[1], reqData[0])

		err = router.SendFrame(reqData[0], goczmq.FlagMore)
		if err != nil {
			continue
		}

		_, err = w.Write(reqData[1])
		if err != nil {
			router.SendFrame([]byte("error"), goczmq.FlagNone)
			continue
		}
		w.Flush()
		router.SendFrame([]byte("OK"), goczmq.FlagNone)
	}
}
