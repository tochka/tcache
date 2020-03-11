package transport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tochka/tcached/cache"
)

func NewServer(c cache.Cache, address string) *Server {
	return &Server{
		Cache:   c,
		Address: address,
	}
}

type Server struct {
	Cache      cache.Cache
	Address    string
	inShutdown int32
	listener   net.Listener
}

func (s *Server) Listen() (err error) {
	var wg sync.WaitGroup

	s.listener, err = net.Listen("tcp", s.Address)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())

	for {
		var conn net.Conn
		conn, err = s.listener.Accept()
		if err != nil {
			if atomic.LoadInt32(&s.inShutdown) == 1 {
				err = nil
			}
			break
		}
		wg.Add(1)
		go worker(conn, s.Cache, &wg, ctx)
	}

	cancel()

	wg.Wait()
	return err
}

func (s *Server) Shotdown() {
	atomic.StoreInt32(&s.inShutdown, 1)
	s.listener.Close()
}

const (
	ItemHeaderSize      = 10 //binary.Size(ItemHeader{})
	OperationHeaderSize = 5  // binary.Size(OperationHeader{})
)

func worker(conn net.Conn, c cache.Cache, wg *sync.WaitGroup, ctx context.Context) {
	var (
		opHederBuf [OperationHeaderSize]byte
		oh         OperationHeader
		op         Operation

		reader = bufio.NewReader(conn)
		writer = bufio.NewWriter(conn)
	)

	defer func() {
		conn.Close()
		wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if _, err := io.ReadFull(reader, opHederBuf[:]); err != nil {
				return
			}

			oh.Code = OpCode(opHederBuf[0])
			oh.PayloadLen = binary.BigEndian.Uint32(opHederBuf[1:])

			payload := make([]byte, int(oh.PayloadLen))
			if _, err := io.ReadFull(reader, payload); err != nil {
				return
			}

			switch oh.Code {
			case OpCodeGet:
				op = get(payload, c)
			case OpCodeSet:
				op = set(payload, c)
			case OpCodeDel:
				op = del(payload, c)
			case OpCodeGetKeys:
				op = getkeys(payload, c)
			default:
				op = Operation{Code: StatusError, Payload: UnsupportedOperationMsg}
			}

			opHederBuf[0] = byte(op.Code)
			binary.BigEndian.PutUint32(opHederBuf[1:], uint32(len(op.Payload)))

			if _, err := writer.Write(opHederBuf[:]); err != nil {
				return
			}
			if _, err := writer.Write(op.Payload); err != nil {
				return
			}
			writer.Flush()
		}
	}
}

func getkeys(_ []byte, c cache.Cache) Operation {
	var b [8]byte // int64

	keys := c.Keys()
	buf := bytes.NewBuffer(nil)

	binary.BigEndian.PutUint64(b[:], uint64(len(keys)))
	buf.Write(b[:])
	for _, k := range keys {
		binary.BigEndian.PutUint16(b[:], uint16(len(k)))
		buf.Write(b[:2])
		buf.WriteString(k)
	}
	return Operation{Code: StatusSuccess, Payload: buf.Bytes()}
}

func del(payload []byte, c cache.Cache) Operation {
	key := string(payload)
	c.Remove(key)

	return Operation{Code: StatusSuccess}
}

func get(payload []byte, c cache.Cache) Operation {
	key := string(payload)
	val, ok := c.Get(key)
	if !ok {
		return Operation{Code: StatusNotFound}
	}
	if time.Now().UTC().Unix() >= int64(val.Expired) {
		return Operation{Code: StatusNotFound}
	}

	return Operation{Code: StatusSuccess, Payload: EncodeItem(key, val)}
}

func set(payload []byte, c cache.Cache) Operation {
	key, v, err := ParseItem(payload)
	if err != nil {
		return Operation{Code: StatusError, Payload: []byte(err.Error())}
	}

	if time.Now().UTC().Unix() >= int64(v.Expired) {
		return Operation{Code: StatusSuccess}
	}

	c.Add(key, v)

	return Operation{Code: StatusSuccess}
}

func EncodeItem(key string, value cache.Value) []byte {
	buf := make([]byte, ItemHeaderSize+len(key)+len(value.Value))
	binary.BigEndian.PutUint32(buf, value.Expired)
	binary.BigEndian.PutUint16(buf[4:], uint16(len(key)))
	binary.BigEndian.PutUint32(buf[6:], uint32(len(value.Value)))
	copy(buf[ItemHeaderSize:], key)
	copy(buf[ItemHeaderSize+len(key):], value.Value)
	return buf
}

func ParseItem(data []byte) (key string, v cache.Value, err error) {
	if len(data) < ItemHeaderSize {
		return key, v, ErrSetItemSizeTooSmall
	}

	var h ItemHeader = ItemHeader{
		Expired:  binary.BigEndian.Uint32(data),
		KeyLen:   binary.BigEndian.Uint16(data[4:]),
		ValueLen: binary.BigEndian.Uint32(data[6:]),
	}
	data = data[ItemHeaderSize:]

	if len(data) != int(h.KeyLen)+int(h.ValueLen) {
		return key, v, ErrSetItemSizeTooSmall
	}

	key = string(data[:h.KeyLen])
	v.Expired = h.Expired
	v.Value = make([]byte, int(h.ValueLen))
	copy(v.Value, data[int(h.KeyLen):])

	return key, v, nil
}

type ItemHeader struct {
	Expired  uint32
	ValueLen uint32
	KeyLen   uint16
}

type OperationHeader struct {
	Code       OpCode
	PayloadLen uint32
}

type Operation struct {
	Code    OpCode
	Payload []byte
}

type OpCode byte

const (
	OpCodeUnknown OpCode = iota
	OpCodeSet
	OpCodeGet
	OpCodeDel
	OpCodeGetKeys
)
const (
	StatusSuccess OpCode = 0xFF - iota
	StatusError
	StatusNotFound
)

var (
	UnsupportedOperationMsg = []byte("unsupported operation")
	ErrSetItemSizeTooSmall  = errors.New("set item size too small")
	ErrSetItemSizeIncorrect = errors.New("set item size incorrect")
)
