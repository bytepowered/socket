package ext

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

type (
	OnDialFunc  func(config SocketConfig) (net.Conn, error)           // 创建连接函数
	OnOpenFunc  func(conn net.Conn, config SocketConfig) error        // 连接成功后的回调函数
	OnCloseFunc func(conn net.Conn)                                   // 连接关闭后的回调函数
	OnReadFunc  func(conn net.Conn) error                             // 读取连接数据函数
	OnErrorFunc func(so *SocketConnector, err error) (continued bool) // 连接错误处理函数，返回True继续读取当前连接；False则重新建立连接；
)

type ConnState uint32

const (
	ConnStateConnecting ConnState = iota
	ConnStateConnected
	ConnStateDisconnecting
	ConnStateDisconnected
)

type SocketConfig struct {
	Network       string `toml:"network"`
	RemoteAddress string `toml:"address"`
	BindAddress   string `toml:"bind"`
	// Reconnect
	RetryMax   int           `toml:"retry-max"`
	RetryDelay time.Duration `toml:"retry-delay"`
	// TCP
	TCPNoDelay   bool `toml:"tcp-no-delay"`
	TCPKeepAlive uint `toml:"tcp-keep-alive"`
	// SocketConnector
	ReadTimeout time.Duration `toml:"read-timeout"`
}

type SocketConnectorOptions func(c *SocketConnector)

type SocketConnector struct {
	onOpenFunc  OnOpenFunc
	onCloseFunc OnCloseFunc
	onReadFunc  OnReadFunc
	onErrFunc   OnErrorFunc
	onDialFunc  OnDialFunc
	config      SocketConfig
	downctx     context.Context
	downfun     context.CancelFunc
	conn        net.Conn
	state       uint32
}

func NewSocketConnector(config SocketConfig, opts ...SocketConnectorOptions) *SocketConnector {
	ctx, fun := context.WithCancel(context.TODO())
	so := &SocketConnector{
		downctx:     ctx,
		downfun:     fun,
		config:      config,
		onDialFunc:  OnTCPDialFunc,
		onOpenFunc:  OnTCPOpenFunc,
		onCloseFunc: func(_ net.Conn) {},
	}
	for _, opt := range opts {
		opt(so)
	}
	return so
}

func (nc *SocketConnector) SetDialFunc(f OnDialFunc) *SocketConnector {
	nc.onDialFunc = f
	return nc
}

func (nc *SocketConnector) SetOpenFunc(f OnOpenFunc) *SocketConnector {
	nc.onOpenFunc = f
	return nc
}

func (nc *SocketConnector) SetCloseFunc(f OnCloseFunc) *SocketConnector {
	nc.onCloseFunc = f
	return nc
}

func (nc *SocketConnector) SetRecvFunc(f OnReadFunc) *SocketConnector {
	nc.onReadFunc = f
	return nc
}

func (nc *SocketConnector) OnErrorFunc(f OnErrorFunc) *SocketConnector {
	nc.onErrFunc = f
	return nc
}

func (nc *SocketConnector) State() ConnState {
	return ConnState(nc.state)
}

func (nc *SocketConnector) Serve() {
	assert(nc.onDialFunc, "'onDialFunc' is required")
	assert(nc.onErrFunc, "'onErrFunc' is required")
	assert(nc.onDialFunc, "'onDialFunc' is required")
	assert(nc.onOpenFunc, "'onOpenFunc' is required")
	assert(nc.onCloseFunc, "'onCloseFunc' is required")
	nc.config.ReadTimeout = nc.ensure(nc.config.ReadTimeout, time.Second, time.Second*10)
	nc.config.RetryDelay = nc.ensure(nc.config.RetryDelay, time.Second, time.Second*5)
	var count int
	var delay time.Duration
	refresh := func(conn net.Conn) {
		if nc.conn != nil {
			_ = nc.conn.Close()
		}
		nc.conn = conn
		count = 0
		delay = time.Millisecond * 5
	}
	reconnect := func() {
		if nc.config.RetryMax > 0 && count >= nc.config.RetryMax {
			nc.setConnState(ConnStateDisconnecting)
		} else {
			count++
			nc.setConnState(ConnStateConnecting)
			time.Sleep(nc.config.RetryDelay)
		}
	}
	checkerr := func(err error) {
		select {
		case <-nc.downctx.Done():
			nc.setConnState(ConnStateDisconnecting)
			return
		default:
			// next
		}
		if errors.Is(err, io.EOF) {
			reconnect()
		} else if nc.onErrFunc(nc, err) {
			return
		} else {
			reconnect()
		}
	}
	defer nc.Close()
	nc.setConnState(ConnStateConnecting)
	for {
		select {
		case <-nc.Done():
			return
		default:
			// next
		}
		switch ConnState(atomic.LoadUint32(&nc.state)) {
		case ConnStateDisconnecting:
			return
		case ConnStateConnecting:
			if conn, err := nc.onDialFunc(nc.config); err != nil {
				checkerr(fmt.Errorf("connection dial: %w", err))
			} else if err = nc.onOpenFunc(conn, nc.config); err != nil {
				checkerr(fmt.Errorf("connection open: %w", err))
			} else {
				refresh(conn)
				nc.setConnState(ConnStateConnected)
			}
		case ConnStateConnected:
			if err := nc.conn.SetReadDeadline(time.Now().Add(nc.config.ReadTimeout)); err != nil {
				checkerr(fmt.Errorf("connection set read options: %w", err))
			} else if err = nc.onReadFunc(nc.conn); err != nil {
				var terr = err
				if werr := errors.Unwrap(err); werr != nil {
					terr = werr
				}
				if nerr, ok := terr.(net.Error); ok && (nerr.Temporary() || nerr.Timeout()) {
					time.Sleep(nc.ensure(delay, time.Millisecond*5, time.Millisecond*100))
				} else {
					checkerr(err)
				}
			}
		}
	}
}

func (nc *SocketConnector) Shutdown() {
	nc.downfun()
}

func (nc *SocketConnector) Done() <-chan struct{} {
	return nc.downctx.Done()
}

func (nc *SocketConnector) Close() {
	nc.close0()
}

func (nc *SocketConnector) setConnState(s ConnState) {
	if s == ConnStateDisconnecting && nc.conn != nil {
		_ = nc.conn.SetDeadline(time.Time{})
	}
	nc.state = uint32(s)
}

func (nc *SocketConnector) close0() {
	defer nc.setConnState(ConnStateDisconnected)
	if nc.conn == nil {
		return
	}
	defer nc.onCloseFunc(nc.conn)
	if err := nc.conn.Close(); err != nil && !strings.Contains(err.Error(), "closed network connection") {
		nc.onErrFunc(nc, fmt.Errorf("connection close: %w", err))
	}
}

func (nc *SocketConnector) ensure(v time.Duration, def, max time.Duration) time.Duration {
	if v == 0 {
		v = def
	} else {
		v *= 2
	}
	if v > max {
		v = max
	}
	return v
}

func WithOpenFunc(f OnOpenFunc) SocketConnectorOptions {
	return func(c *SocketConnector) {
		c.onOpenFunc = f
	}
}

func WithReadFunc(f OnReadFunc) SocketConnectorOptions {
	return func(c *SocketConnector) {
		c.onReadFunc = f
	}
}

func WithErrorFunc(f OnErrorFunc) SocketConnectorOptions {
	return func(c *SocketConnector) {
		c.onErrFunc = f
	}
}

func WithDailFunc(f OnDialFunc) SocketConnectorOptions {
	return func(c *SocketConnector) {
		c.onDialFunc = f
	}
}

func OnTCPOpenFunc(conn net.Conn, config SocketConfig) (err error) {
	network := conn.RemoteAddr().Network()
	socket, ok := conn.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("conn is not tcp connection, was: %s", network)
	}
	if strings.HasPrefix(network, "tcp") {
		if err = socket.SetNoDelay(config.TCPNoDelay); err != nil {
			return err
		}
		if config.TCPKeepAlive > 0 {
			_ = socket.SetKeepAlive(true)
			err = socket.SetKeepAlivePeriod(time.Second * time.Duration(config.TCPKeepAlive))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func OnTCPDialFunc(opts SocketConfig) (net.Conn, error) {
	if !strings.HasPrefix(opts.Network, "tcp") {
		return nil, fmt.Errorf("'network' requires 'tcp' protocols")
	}
	laddr, _ := netResolveTCPAddr(opts.Network, opts.BindAddress)
	raddr, err := netResolveTCPAddr(opts.Network, opts.RemoteAddress)
	if err != nil {
		return nil, err
	}
	return net.DialTCP(opts.Network, laddr, raddr)
}

func netResolveTCPAddr(network, address string) (*net.TCPAddr, error) {
	if address == "" {
		return nil, fmt.Errorf("'address' is required")
	}
	if addr, err := net.ResolveTCPAddr(network, address); err != nil {
		return nil, err
	} else {
		return addr, nil
	}
}
