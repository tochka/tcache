package client

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/tochka/tcached/cache"
	"github.com/tochka/tcached/transport"
)

type Value cache.Value

var (
	ErrEntityNotFound   = errors.New("entity not found")
	ErrConnectionClosed = errors.New("connection closed")
)

func NewClient(address string) *Client {
	p := newPool(address, 5, 10, 0)
	return &Client{p}
}

type Client struct {
	p *pool
}

func (c *Client) Remove(key string) error {
	_, err := c.rpc(transport.Operation{Code: transport.OpCodeDel, Payload: []byte(key)})
	return err
}

func (c *Client) Get(key string) (value Value, err error) {
	data, err := c.rpc(transport.Operation{Code: transport.OpCodeGet, Payload: []byte(key)})
	if err != nil {
		return value, err
	}

	var v cache.Value
	key, v, err = transport.ParseItem(data)
	if err != nil {
		return value, err
	}
	return Value(v), nil
}

func (c *Client) Add(key string, value Value) error {
	_, err := c.rpc(transport.Operation{Code: transport.OpCodeSet, Payload: transport.EncodeItem(key, cache.Value(value))})
	return err
}

func (c *Client) Keys() ([]string, error) {
	data, err := c.rpc(transport.Operation{Code: transport.OpCodeGetKeys})
	if err != nil {
		return nil, err
	}

	var keysLen uint64
	var keyLen int
	keysLen = binary.BigEndian.Uint64(data)
	data = data[8:]

	keys := make([]string, int(keysLen))
	for i := range keys {
		keyLen = int(binary.BigEndian.Uint16(data))
		data = data[2:]
		keys[i] = string(data[:keyLen])
		data = data[keyLen:]
	}
	return keys, nil
}

func (c *Client) Close() {
	c.p.close()
}

func (c *Client) rpc(op transport.Operation) ([]byte, error) {
	conn, err := c.p.get()
	if err != nil {
		return nil, err
	}

	if err := c.write(op, conn); err != nil {
		c.p.remove(conn)
		return nil, err
	}

	if op, err = c.read(conn); err != nil {
		c.p.remove(conn)
		return nil, err
	}
	c.p.put(conn)

	if op.Code == transport.StatusSuccess {
		return op.Payload, nil
	}
	if op.Code == transport.StatusNotFound {
		return nil, ErrEntityNotFound
	}
	return nil, errors.New(string(op.Payload))
}

func (c *Client) read(conn net.Conn) (op transport.Operation, err error) {
	var (
		opHederBuf [transport.OperationHeaderSize]byte
		payloadLen uint32
	)
	if _, err := io.ReadFull(conn, opHederBuf[:]); err != nil {
		return op, err
	}

	op.Code = transport.OpCode(opHederBuf[0])
	payloadLen = binary.BigEndian.Uint32(opHederBuf[1:])

	op.Payload = make([]byte, int(payloadLen))
	if _, err := io.ReadFull(conn, op.Payload); err != nil {
		return op, err
	}
	return op, nil
}

func (c *Client) write(op transport.Operation, conn net.Conn) error {
	var opHederBuf [transport.OperationHeaderSize]byte

	opHederBuf[0] = byte(op.Code)
	binary.BigEndian.PutUint32(opHederBuf[1:], uint32(len(op.Payload)))

	if _, err := conn.Write(opHederBuf[:]); err != nil {
		return err
	}

	if _, err := conn.Write(op.Payload); err != nil {
		return err
	}
	return nil
}

var DefaultWaitConn = 100 * time.Millisecond

func newPool(address string, maxIdlConns int, maxConns int, waitConn time.Duration) *pool {
	if maxIdlConns == 0 {
		maxIdlConns = 0xFFFFFFFF
	}
	if maxConns == 0 {
		maxConns = 0xFFFFFFFF
	}
	if waitConn == 0 {
		waitConn = DefaultWaitConn
	}
	p := &pool{
		address:      address,
		maxConns:     int32(maxConns),
		maxIdleConns: int32(maxIdlConns),
		waitFreeConn: waitConn,
		connCh:       make(chan net.Conn, maxConns),
	}
	go p.run()
	return p
}

type pool struct {
	address string

	maxIdleConns int32
	maxConns     int32
	currentConns int32
	connCh       chan net.Conn
	waitFreeConn time.Duration

	closed int32
}

func (p *pool) get() (net.Conn, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return nil, errors.New("connection pool was closed")
	}
	select {
	case conn, ok := <-p.connCh:
		if !ok {
			return nil, errors.New("connection pool was closed")
		}
		return conn, nil
	default:
		nv := atomic.AddInt32(&p.currentConns, 1)
		if nv < p.maxConns {
			conn, err := net.Dial("tcp", p.address)
			if err != nil {
				return nil, err
			}
			return &poolConn{Conn: conn, lastUsed: time.Now().Unix()}, nil
		}
		atomic.AddInt32(&p.currentConns, -1)

		timer := time.NewTimer(p.waitFreeConn)
		select {
		case <-timer.C:
			timer.Stop()
			return nil, errors.New("all connections are used")
		case conn, ok := <-p.connCh:
			timer.Stop()
			if !ok {
				return nil, errors.New("connection pool was closed")
			}
			return conn, nil
		}
	}
}

func (p *pool) put(conn net.Conn) {
	if atomic.LoadInt32(&p.closed) == 1 {
		p.remove(conn)
		return
	}
	p.connCh <- conn
}

func (p *pool) remove(conn net.Conn) {
	atomic.AddInt32(&p.currentConns, -1)
	conn.Close()
}

func (p *pool) close() {
	atomic.StoreInt32(&p.closed, 1)

	for {
		select {
		case conn := <-p.connCh:
			p.remove(conn)
		default:
			close(p.connCh)
			return
		}
	}
}

func (p *pool) run() {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for _ = range timer.C {

		if atomic.LoadInt32(&p.closed) == 1 {
			return
		}

		conn, ok := <-p.connCh
		if !ok {
			return
		}
		pc := conn.(*poolConn)
		// the oldest connection used
		if time.Now().Add(5*time.Second).Unix() < atomic.LoadInt64(&pc.lastUsed) {
			p.put(conn)
			continue
		}

		if atomic.LoadInt32(&p.currentConns) >= p.maxIdleConns {
			p.remove(conn)
		}
	}
}

type poolConn struct {
	net.Conn

	lastUsed int64
}

func (c *poolConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if err == nil {
		atomic.StoreInt64(&c.lastUsed, time.Now().Unix())
	}
	return
}

func (c *poolConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	if err == nil {
		atomic.StoreInt64(&c.lastUsed, time.Now().Unix())
	}
	return
}
