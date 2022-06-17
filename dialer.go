package netx

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
	StateConnecting    SocketState = iota // 未连接状态，正在建立连接中
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

func (sd *SocketDialer) SetDialFunc(f OnDialFunc) *SocketDialer {
	sd.onDialFunc = f
	return sd
}

func (sd *SocketDialer) SetOpenFunc(f OnOpenFunc) *SocketDialer {
	sd.onOpenFunc = f
	return sd
}

func (sd *SocketDialer) SetCloseFunc(f OnCloseFunc) *SocketDialer {
	sd.onCloseFunc = f
	return sd
}

func (sd *SocketDialer) SetRecvFunc(f OnReadFunc) *SocketDialer {
	sd.onReadFunc = f
	return sd
}

func (sd *SocketDialer) OnErrorFunc(f OnErrorFunc) *SocketDialer {
	sd.onErrorFunc = f
	return sd
}

func (sd *SocketDialer) State() SocketState {
	return SocketState(sd.state)
}

func (sd *SocketDialer) Serve() {
	assert(sd.onDialFunc, "'onDialFunc' is required")
	assert(sd.onErrorFunc, "'onErrorFunc' is required")
	assert(sd.onDialFunc, "'onDialFunc' is required")
	assert(sd.onOpenFunc, "'onOpenFunc' is required")
	assert(sd.onCloseFunc, "'onCloseFunc' is required")
	sd.config.ReadTimeout = sd.incr(sd.config.ReadTimeout, time.Second, time.Second*10)
	sd.config.RetryDelay = sd.incr(sd.config.RetryDelay, time.Second, time.Second*5)
	var retries int
	var delay time.Duration
	reset := func(newconn net.Conn) {
		if sd.conn != nil {
			_ = sd.conn.Close()
		}
		sd.conn = newconn
		retries = 0
		delay = time.Millisecond * 5
	}
	reconnect := func() {
		if sd.config.RetryMax > 0 && retries >= sd.config.RetryMax {
			sd.setState(SocketStateClosing)
		} else {
			retries++
			sd.ResetState()
			time.Sleep(sd.config.RetryDelay)
		}
	}
	retry := func(err error) {
		select {
		case <-sd.downctx.Done():
			sd.setState(SocketStateClosing)
		default:
			// next
		}
		if errors.Is(err, io.EOF) {
			reconnect()
		}
		if sd.onErrorFunc(sd, err) {
			reconnect()
		}
	}
	defer sd.Close()
	sd.ResetState()
	for {
		select {
		case <-sd.Done():
			return
		default:
			// next
		}
		switch SocketState(atomic.LoadUint32(&sd.state)) {
		case SocketStateClosing, SocketStateClosed:
			return
		case StateConnecting:
			if conn, err := sd.onDialFunc(sd.config); err != nil {
				retry(fmt.Errorf("connection dial: %w", err))
			} else if err = sd.onOpenFunc(conn, sd.config); err != nil {
				retry(fmt.Errorf("connection open: %w", err))
			} else {
				reset(conn)
				sd.setState(StateOpen)
			}
		case StateOpen:
			if err := sd.conn.SetReadDeadline(time.Now().Add(sd.config.ReadTimeout)); err != nil {
				retry(fmt.Errorf("connection set read options: %w", err))
			} else if err = sd.onReadFunc(sd.conn); err != nil {
				var terr = err
				if werr := errors.Unwrap(err); werr != nil {
					terr = werr
				}
				if nerr, ok := terr.(net.Error); ok && (nerr.Temporary() || nerr.Timeout()) {
					time.Sleep(sd.incr(delay, time.Millisecond*5, time.Millisecond*100))
				} else {
					retry(err)
				}
			}
		}
	}
}

func (sd *SocketDialer) Shutdown() {
	sd.downfun()
}

func (sd *SocketDialer) Done() <-chan struct{} {
	return sd.downctx.Done()
}

func (sd *SocketDialer) Close() {
	sd.close0()
}

func (sd *SocketDialer) ResetState() {
	sd.setState(StateConnecting)
}

func (sd *SocketDialer) setState(s SocketState) {
	if s == SocketStateClosing && sd.conn != nil {
		_ = sd.conn.SetDeadline(time.Time{})
	}
	atomic.StoreUint32(&sd.state, uint32(s))
}

func (sd *SocketDialer) close0() {
	defer sd.setState(SocketStateClosed)
	if sd.conn == nil {
		return
	}
	defer sd.onCloseFunc(sd.conn)
	err := sd.conn.Close()
	if err != nil && !strings.Contains(err.Error(), "closed network connection") {
		sd.onErrorFunc(sd, fmt.Errorf("connection close: %w", err))
	}
}

func (sd *SocketDialer) incr(v time.Duration, def, max time.Duration) time.Duration {
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

func WithCloseFunc(f OnCloseFunc) SocketDialerOptions {
	return func(c *SocketDialer) {
		c.onCloseFunc = f
	}
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
