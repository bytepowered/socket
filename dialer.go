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
	OnDialFunc  func(config SocketConfig) (net.Conn, error)        // 创建连接函数
	OnOpenFunc  func(conn net.Conn, config SocketConfig) error     // 连接成功后的回调函数
	OnCloseFunc func(conn net.Conn)                                // 连接关闭后的回调函数
	OnReadFunc  func(conn net.Conn) error                          // 读取连接数据函数
	OnErrorFunc func(so *SocketDialer, err error) (continued bool) // 连接错误处理函数，返回True继续读取当前连接；False则重新建立连接；
)

type SocketState uint32

const (
	StateConnecting    SocketState = iota // 正在建立连接中
	StateOpen                             // 已建立连接并且可以读取数据
	SocketStateClosing                    // 连接正在关闭
	SocketStateClosed                     // 连接已关闭或者没有链接成功
)

type SocketConfig struct {
	Network       string `json:"network"`
	RemoteAddress string `json:"address"`
	BindAddress   string `json:"bind"`
	// Reconnect
	RetryMax   int           `json:"retry_max"`
	RetryDelay time.Duration `json:"retry_delay"`
	// TCP
	TCPNoDelay   bool          `json:"tcp_no_delay"`
	TCPKeepAlive uint          `json:"tcp_keep_alive"`
	ReadTimeout  time.Duration `json:"read_timeout"`
}

type SocketDialerOptions func(c *SocketDialer)

type SocketDialer struct {
	onOpenFunc  OnOpenFunc
	onCloseFunc OnCloseFunc
	onReadFunc  OnReadFunc
	onErrorFunc OnErrorFunc
	onDialFunc  OnDialFunc
	config      SocketConfig
	downctx     context.Context
	downfun     context.CancelFunc
	conn        net.Conn
	state       uint32
}

func NewSocketDialer(config SocketConfig, opts ...SocketDialerOptions) *SocketDialer {
	ctx, fun := context.WithCancel(context.TODO())
	so := &SocketDialer{
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

func (nc *SocketDialer) SetDialFunc(f OnDialFunc) *SocketDialer {
	nc.onDialFunc = f
	return nc
}

func (nc *SocketDialer) SetOpenFunc(f OnOpenFunc) *SocketDialer {
	nc.onOpenFunc = f
	return nc
}

func (nc *SocketDialer) SetCloseFunc(f OnCloseFunc) *SocketDialer {
	nc.onCloseFunc = f
	return nc
}

func (nc *SocketDialer) SetRecvFunc(f OnReadFunc) *SocketDialer {
	nc.onReadFunc = f
	return nc
}

func (nc *SocketDialer) OnErrorFunc(f OnErrorFunc) *SocketDialer {
	nc.onErrorFunc = f
	return nc
}

func (nc *SocketDialer) State() SocketState {
	return SocketState(nc.state)
}

func (nc *SocketDialer) Serve() {
	assert(nc.onDialFunc, "'onDialFunc' is required")
	assert(nc.onErrorFunc, "'onErrorFunc' is required")
	assert(nc.onDialFunc, "'onDialFunc' is required")
	assert(nc.onOpenFunc, "'onOpenFunc' is required")
	assert(nc.onCloseFunc, "'onCloseFunc' is required")
	nc.config.ReadTimeout = nc.incr(nc.config.ReadTimeout, time.Second, time.Second*10)
	nc.config.RetryDelay = nc.incr(nc.config.RetryDelay, time.Second, time.Second*5)
	var retry int
	var delay time.Duration
	refresh := func(newconn net.Conn) {
		if nc.conn != nil {
			_ = nc.conn.Close()
		}
		nc.conn = newconn
		retry = 0
		delay = time.Millisecond * 5
	}
	reconnect := func() {
		if nc.config.RetryMax > 0 && retry >= nc.config.RetryMax {
			nc.setState(SocketStateClosing)
		} else {
			retry++
			nc.setState(StateConnecting)
			time.Sleep(nc.config.RetryDelay)
		}
	}
	checkerr := func(err error) {
		select {
		case <-nc.downctx.Done():
			nc.setState(SocketStateClosing)
			return
		default:
			// next
		}
		if errors.Is(err, io.EOF) {
			reconnect()
		} else if nc.onErrorFunc(nc, err) {
			return
		} else {
			reconnect()
		}
	}
	defer nc.Close()
	nc.setState(StateConnecting)
	for {
		select {
		case <-nc.Done():
			return
		default:
			// next
		}
		switch SocketState(atomic.LoadUint32(&nc.state)) {
		case SocketStateClosing:
			return
		case StateConnecting:
			if conn, err := nc.onDialFunc(nc.config); err != nil {
				checkerr(fmt.Errorf("connection dial: %w", err))
			} else if err = nc.onOpenFunc(conn, nc.config); err != nil {
				checkerr(fmt.Errorf("connection open: %w", err))
			} else {
				refresh(conn)
				nc.setState(StateOpen)
			}
		case StateOpen:
			if err := nc.conn.SetReadDeadline(time.Now().Add(nc.config.ReadTimeout)); err != nil {
				checkerr(fmt.Errorf("connection set read options: %w", err))
			} else if err = nc.onReadFunc(nc.conn); err != nil {
				var terr = err
				if werr := errors.Unwrap(err); werr != nil {
					terr = werr
				}
				if nerr, ok := terr.(net.Error); ok && (nerr.Temporary() || nerr.Timeout()) {
					time.Sleep(nc.incr(delay, time.Millisecond*5, time.Millisecond*100))
				} else {
					checkerr(err)
				}
			}
		}
	}
}

func (nc *SocketDialer) Shutdown() {
	nc.downfun()
}

func (nc *SocketDialer) Done() <-chan struct{} {
	return nc.downctx.Done()
}

func (nc *SocketDialer) Close() {
	nc.close0()
}

func (nc *SocketDialer) setState(s SocketState) {
	if s == SocketStateClosing && nc.conn != nil {
		_ = nc.conn.SetDeadline(time.Time{})
	}
	atomic.StoreUint32(&nc.state, uint32(s))
}

func (nc *SocketDialer) close0() {
	defer nc.setState(SocketStateClosed)
	if nc.conn == nil {
		return
	}
	defer nc.onCloseFunc(nc.conn)
	if err := nc.conn.Close(); err != nil && !strings.Contains(err.Error(), "closed network connection") {
		nc.onErrorFunc(nc, fmt.Errorf("connection close: %w", err))
	}
}

func (nc *SocketDialer) incr(v time.Duration, def, max time.Duration) time.Duration {
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

func WithOpenFunc(f OnOpenFunc) SocketDialerOptions {
	return func(c *SocketDialer) {
		c.onOpenFunc = f
	}
}

func WithReadFunc(f OnReadFunc) SocketDialerOptions {
	return func(c *SocketDialer) {
		c.onReadFunc = f
	}
}

func WithErrorFunc(f OnErrorFunc) SocketDialerOptions {
	return func(c *SocketDialer) {
		c.onErrorFunc = f
	}
}

func WithDailFunc(f OnDialFunc) SocketDialerOptions {
	return func(c *SocketDialer) {
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
