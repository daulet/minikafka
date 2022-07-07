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
	"syscall"
	"time"
)

const maxTimeouts = 5

/*
 * Broker process is to be booted on each node of deployment.
 */
type Broker struct {
	pubPort        int
	subPort        int
	storageDir     string
	pollingTimeout time.Duration

	knownTopics     map[string]topicChannel
	knownTopicsLock sync.RWMutex

	debug bool
}

type BrokerConfig func(b *Broker)

type topicChannel struct {
	name string
	ch   chan []byte
}

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
	b.knownTopics = make(map[string]topicChannel)
	b.debug = os.Getenv("DEBUG") == "1"
	return b
}

func (b *Broker) Logf(format string, args ...interface{}) {
	if b.debug {
		log.Printf(format, args...)
	}
}

func (b *Broker) Run(ctx context.Context) {
	var (
		wg     sync.WaitGroup
		topics = make(chan topicChannel)
	)

	// listen for publishers, persist, acknowledge
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.writeTopics(ctx, topics)
		}()

		// TODO config broker IP
		lstr, err := net.Listen("tcp", fmt.Sprintf(":%d", b.pubPort))
		if err != nil {
			log.Fatal(err)
		}
		defer lstr.Close()

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.poll(ctx, lstr.(*net.TCPListener), topics)
		}()
	}

	// listen for subscribers, publish
	{
		lstr, err := net.Listen("tcp", fmt.Sprintf(":%d", b.subPort))
		if err != nil {
			log.Fatal(err)
		}
		defer lstr.Close()

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.serveSubs(ctx, lstr.(*net.TCPListener), topics)
		}()
	}

	wg.Wait()
}

func (b *Broker) poll(ctx context.Context, lstr *net.TCPListener, topics chan<- topicChannel) {
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
			b.handle(ctx, conn.(*net.TCPConn), topics)
		}()
	}
	wg.Wait()
}

func (b *Broker) handle(ctx context.Context, conn *net.TCPConn, topics chan<- topicChannel) {
	reader := NewMessageReader[[]byte](conn)
	defer reader.Close()

	reader.SetDeadline(time.Now().Add(b.pollingTimeout))
	data, err := reader.ReadPayload()
	if err != nil {
		b.Logf("failed to read topic of publisher: %v\n", err)
		return
	}
	topic := string(data)
	msgCh, err := b.ensureTopic(topics, topic)
	if err != nil {
		b.Logf("%v sent invalid topic: %v\n", conn.RemoteAddr(), err)
		return
	}

	timeouts := maxTimeouts
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
				timeouts -= 1
				if timeouts == 0 {
					return
				}
				continue
			}
			if err != io.EOF {
				b.Logf("failed to decode message: %v\n", err)
			}
			return
		}
		timeouts = maxTimeouts

		select {
		// TODO decide if acks should be ordered, how does kafka do it?
		case msgCh <- raw:
			// TODO different timeout from polling?
			reader.SetDeadline(time.Now().Add(b.pollingTimeout))
			_, err := reader.writeBytes([]byte("K"))
			if err != nil {
				b.Logf("failed to ack message: %v\n", err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (b *Broker) ensureTopic(topics chan<- topicChannel, topic string) (chan<- []byte, error) {
	if topic == "" {
		return nil, fmt.Errorf("empty topic is invalid")
	}

	b.knownTopicsLock.RLock()
	if tc, ok := b.knownTopics[topic]; ok {
		b.knownTopicsLock.RUnlock()
		return tc.ch, nil
	}
	b.knownTopicsLock.RUnlock()

	// Topic does not exist
	b.knownTopicsLock.Lock()
	defer b.knownTopicsLock.Unlock()

	// check once more for subsequent visitors
	if tc, ok := b.knownTopics[topic]; ok {
		return tc.ch, nil
	}

	logFile, err := os.OpenFile(fmt.Sprintf("%s/%s.log", b.storageDir, topic), os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Fatal(err)
	}
	logFile.Close()

	tc := topicChannel{
		name: topic,
		ch:   make(chan []byte),
	}
	b.knownTopics[topic] = tc
	// TODO should this be under a lock?
	topics <- tc
	return tc.ch, nil
}

func (b *Broker) writeTopics(ctx context.Context, topics <-chan topicChannel) {
	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case topic := <-topics:
			wg.Add(1)
			go func() {
				defer wg.Done()
				b.write(ctx, topic.name, topic.ch)
			}()
		}
	}
}

func (b *Broker) write(ctx context.Context, topic string, msgCh <-chan []byte) {
	// don't pass O_CREATE, file is ensured upstream, panic otherwise
	f, err := os.OpenFile(fmt.Sprintf("%s/%s.log", b.storageDir, topic), os.O_WRONLY|os.O_APPEND, 0660)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	t := time.After(10 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			w.Flush()
			return
		case <-t:
			// TODO this affects overall e2e latency
			w.Flush()
			// TODO bench for optimal e2e latency
			t = time.After(10 * time.Millisecond)
		case msg := <-msgCh:
			_, err := w.Write(msg)
			if err != nil {
				// TODO send a nack
				continue
			}
		}
	}
}

func (b *Broker) serveSubs(ctx context.Context, lstr *net.TCPListener, topics chan<- topicChannel) {
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
			b.publish(ctx, conn, topics)
		}()
	}
	wg.Wait()
}

// somehow using larger size reduces throughput
const maxSendfileSize int = 1 << 7

// TODO simplify this to use io.Copy() when sendfile issue is fixed:
// https://github.com/golang/go/issues/53658
func (b *Broker) publish(ctx context.Context, conn net.Conn, topics chan<- topicChannel) {
	reader := NewMessageReader[[]byte](conn)
	defer reader.Close()

	conn.SetReadDeadline(time.Now().Add(b.pollingTimeout))
	data, err := reader.ReadPayload()
	if err != nil {
		b.Logf("failed to decode message: %v\n", err)
		return
	}
	topic := string(data)
	_, err = b.ensureTopic(topics, topic)
	if err != nil {
		b.Logf("%v sent invalid topic: %v\n", conn.RemoteAddr(), err)
		return
	}

	f, err := os.OpenFile(fmt.Sprintf("%s/%s.log", b.storageDir, topic), os.O_APPEND|os.O_RDONLY, 0660)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	sc, err := f.SyscallConn()
	if err != nil {
		log.Fatalf("unable to get syscall conn: %v", err)
	}

	tcpConn := conn.(*net.TCPConn)
	socket, err := tcpConn.File()
	if err != nil {
		log.Fatalf("unable to get conn.File(): %v", err)
	}
	defer socket.Close()

	connCtx, cancel := context.WithCancel(ctx)
	{
		go func(ctx context.Context) {
			buf := make([]byte, 1)
			for {
				// twice as long as subscriber heartbeat interval
				conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				_, err := conn.Read(buf)
				if err != nil {
					cancel()
					return
				}
			}
		}(ctx)
	}
	var (
		offset int64
		werr   error
	)
	for {
		select {
		case <-connCtx.Done():
			return
		default:
		}

		prevOffset := offset
		err = sc.Read(func(fd uintptr) bool {
			_, werr = syscall.Sendfile(int(socket.Fd()), int(fd), &offset, maxSendfileSize)
			return true
		})
		if err == nil {
			err = werr
		}
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			log.Fatalf("unable to send file: %v", err)
		}
		// don't stay on CPU if there are no new messages
		if offset == prevOffset {
			<-time.After(100 * time.Millisecond)
		}
	}
}
