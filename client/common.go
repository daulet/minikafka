package client

import (
	"fmt"
	"log"
	"net"
	"time"
)

func dial(network, addr string, maxTimeout time.Duration) (net.Conn, error) {
	var (
		conn    net.Conn
		err     error
		timeout = time.Millisecond
	)
	for timeout < maxTimeout {
		conn, err = net.DialTimeout(network, addr, timeout)
		if err != nil {
			log.Printf("dial %v: timeout after %v, will retry\n", addr, timeout)
			timeout += timeout
			continue
		}
		break
	}
	if conn == nil {
		return nil, fmt.Errorf("failed to dial after retries up to %v timeout: %v", timeout, err)
	}
	return conn, err
}
