package minikafka

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

/*
	Broker process is to be booted on each node of deployment.
*/
type Broker struct {
	pubPort        int
	subPort        int
	storageDir     string
	pollingTimeout time.Duration
}

type BrokerConfig func(b *Broker)

func BrokerPublishPort(port int) BrokerConfig {
	return func(b *Broker) {
		b.pubPort = port
	}
}

func BrokerSubscribePort(port int) BrokerConfig {
	return func(b *Broker) {
		b.subPort = port
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

	// ensure storage file exists
	{
		logFile, err := os.OpenFile(fmt.Sprintf("%s/broker.log", b.storageDir), os.O_APPEND|os.O_CREATE, 0660)
		if err != nil {
			log.Fatal(err)
		}
		logFile.Close()
	}

	// listen for publishers, persist, acknowledge
	{
		msgCh := make(chan []byte)

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.write(ctx, msgCh)
		}()

		// TODO config broker IP
		lstr, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", b.pubPort))
		if err != nil {
			log.Fatal(err)
		}
		defer lstr.Close()

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.poll(ctx, lstr.(*net.TCPListener), msgCh)
		}()
	}

	// listen for subscribers, publish
	{
		lstr, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", b.subPort))
		if err != nil {
			log.Fatal(err)
		}
		defer lstr.Close()

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.serveSubs(ctx, lstr.(*net.TCPListener))
		}()
	}

	wg.Wait()
}

func (b *Broker) poll(ctx context.Context, lstr *net.TCPListener, msgCh chan<- []byte) {
	var wg sync.WaitGroup
LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		default:
		}

		lstr.SetDeadline(time.Now().Add(b.pollingTimeout))
		conn, err := lstr.Accept()
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				continue
			}
			if err == io.EOF {
				break LOOP
			}
			log.Fatalf("tcp.accept(): %v", err)
		}
		// handle will close the connection, avoid defer per conn in this function
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.handle(ctx, conn.(*net.TCPConn), msgCh)
		}()
	}
	wg.Wait()
}

func (b *Broker) handle(ctx context.Context, conn *net.TCPConn, msgCh chan<- []byte) {
	reader := NewMessageReader(conn)
	defer reader.Close()
	for {
		// separately check for closed context is necessary in case of read timeout
		select {
		case <-ctx.Done():
			return
		default:
		}

		reader.SetDeadline(time.Now().Add(b.pollingTimeout))
		raw, err := reader.ReadRaw()
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				continue
			}
			if err != io.EOF {
				log.Printf("failed to decode message: %v\n", err)
			}
			return
		}

		select {
		// TODO decide if acks should be ordered, how does kafka do it?
		case msgCh <- raw:
			// TODO different timeout from polling?
			reader.SetDeadline(time.Now().Add(b.pollingTimeout))
			_, err := reader.writeBytes([]byte("OK"))
			if err != nil {
				log.Printf("failed to ack message: %v\n", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (b *Broker) write(ctx context.Context, msgCh <-chan []byte) {
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
		case msg := <-msgCh:
			_, err := w.Write(msg)
			if err != nil {
				// TODO send a nack
				continue
			}
			w.Flush()
		}
	}
}

func (b *Broker) serveSubs(ctx context.Context, lstr *net.TCPListener) {
	var wg sync.WaitGroup
LOOP:
	for {
		select {
		case <-ctx.Done():
			break LOOP
		default:
		}

		lstr.SetDeadline(time.Now().Add(b.pollingTimeout))
		conn, err := lstr.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			if err == io.EOF {
				break LOOP
			}
			log.Fatalf("tcp.accept(): %v", err)
		}
		defer conn.Close()

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.publish(ctx, conn)
		}()
	}
	wg.Wait()
}

func (b *Broker) publish(ctx context.Context, conn net.Conn) {
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
		n, err := io.Copy(conn, f)
		if err != nil {
			log.Printf("publish: write failed with %v", err)
			break
		}
		offset += int64(n)
	}
}
