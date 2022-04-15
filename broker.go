package minikafka

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/zeromq/goczmq"
)

/*
	Broker process is to be booted on each node of deployment.
*/
type Broker struct {
	port       int
	storageDir string
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

func NewBroker(opts ...BrokerConfig) *Broker {
	b := &Broker{}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *Broker) Run() {
	router, err := goczmq.NewRouter(fmt.Sprintf("tcp://*:%d", b.port))
	if err != nil {
		log.Fatal(err)
	}
	defer router.Destroy()

	f, err := os.OpenFile(fmt.Sprintf("%s/broker.log", b.storageDir), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		log.Fatal(err)
	}
	w := bufio.NewWriter(f)
	for {
		reqData, err := router.RecvMessage()
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
		router.SendFrame([]byte("OK"), goczmq.FlagNone)
	}
}
