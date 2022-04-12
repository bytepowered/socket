package ext

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestConnection(t *testing.T) {
	conn := NewSocketConnector(SocketConfig{
		Network:       "tcp",
		RemoteAddress: "127.0.0.1:8888",
		RetryMax:      -1,
		RetryDelay:    time.Second,
	}, WithErrorFunc(func(conn *SocketConnector, err error) (continued bool) {
		fmt.Println(err)
		return true
	}), WithReadFunc(func(conn net.Conn) error {
		meta := make([]byte, 3)
		_, err := conn.Read(meta)
		if err != nil {
			return err
		}
		fmt.Println(string(meta))
		return nil
	}))
	time.AfterFunc(time.Minute*5, func() {
		conn.Shutdown()
	})
	conn.Serve()
}
