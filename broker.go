package minikafka

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
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
	var wg sync.WaitGroup
	msgCh := make(chan [][]byte)
	ackCh := make(chan []byte)

	// ensure storage file exists
	{
		logFile, err := os.OpenFile(fmt.Sprintf("%s/broker.log", b.storageDir), os.O_APPEND|os.O_CREATE, 0660)
		if err != nil {
			log.Fatal(err)
		}
		logFile.Close()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		b.write(ctx, msgCh, ackCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		b.publish(ctx)
	}()

	router, err := goczmq.NewRouter(fmt.Sprintf("tcp://*:%d", b.port))
	if err != nil {
		log.Fatal(err)
	}
	defer router.Destroy()

	wg.Add(1)
	go func() {
		defer wg.Done()
		go b.acknowlege(ctx, router, ackCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		b.poll(ctx, router, msgCh)
	}()

	wg.Wait()
}

func (b *Broker) poll(ctx context.Context, router *goczmq.Sock, msgCh chan<- [][]byte) {
	poller, err := goczmq.NewPoller(router)
	if err != nil {
		log.Fatal(err)
	}
	defer poller.Destroy()

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

		msg, err := socket.RecvMessage()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("received %v from %v", msg[1], msg[0])

		msgCh <- msg
	}
}

func (b *Broker) write(ctx context.Context, msgCh <-chan [][]byte, ackCh chan<- []byte) {
	// don't pass O_CREATE, file is ensured upstream, panic otherwise
	f, err := os.OpenFile(fmt.Sprintf("%s/broker.log", b.storageDir), os.O_RDWR|os.O_APPEND, 0660)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	for {
		select {
		case <-ctx.Done():
			w.Flush()
			return
		case data := <-msgCh:
			// data[0] is the sender, data[1] is the message
			_, err := w.Write(data[1])
			if err != nil {
				// TODO send a nack
				continue
			}
			w.Flush()

			ackCh <- data[0]
		}
	}
}

func (b *Broker) publish(ctx context.Context) {
	f, err := os.OpenFile(fmt.Sprintf("%s/broker.log", b.storageDir), os.O_RDONLY, 0660)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var offset int64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		f.Seek(offset, 0)
		n, err := io.Copy(os.Stdout, f)
		if err != nil {
			log.Fatal(err)
		}
		offset += int64(n)
	}
}

func (b *Broker) acknowlege(ctx context.Context, sock *goczmq.Sock, ackCh <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case sender := <-ackCh:
			sock.SendFrame(sender, goczmq.FlagMore)
			sock.SendFrame([]byte("OK"), goczmq.FlagNone)
		}
	}
}
