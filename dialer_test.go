package netx

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestConnection(t *testing.T) {
	dialer := NewSocketDialer(SocketConfig{
		Network:       "tcp",
		RemoteAddress: "127.0.0.1:8888",
		RetryMax:      10,
		RetryDelay:    time.Second,
	}, WithOpenFunc(func(conn net.Conn, config SocketConfig) error {
		fmt.Println("==> On Opened")
		return nil
	}), WithErrorFunc(func(conn *SocketDialer, err error) (continued bool) {
		fmt.Println("==> On Error: " + err.Error())
		return true
	}), WithReadFunc(func(conn net.Conn) error {
		meta := make([]byte, 1024)
		n, err := conn.Read(meta)
		if err != nil {
			return err
		}
		fmt.Println("==> On Read: " + string(meta[:n]))
		return nil
	}))
	dialer.SetCloseFunc(func(conn net.Conn) {
		dialer.ResetState()
	})
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	go func() {
		for {
			<-ticker.C
			dialer.Close()
		}
	}()
	time.AfterFunc(time.Minute*5, func() {
		dialer.Shutdown()
	})
	dialer.Serve()
}
