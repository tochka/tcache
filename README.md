# Tcached (test task)

Tcached is a LRU cache server which is working by TCP protocol.

## Install

### Required

* GO 1.13
* git

### Setup

Download the project ``git clone github.com/tochka/tcached``

Build ``go build``

## Settings

Command line |Default value| Description
-------------|-------------|------------
address | :30003 | Default listening address and port
max-entries | 0 | Amount of entries in the server (0 is unlimited).

> if you reach the maximum of entries the server drop the most unused item

## API

Tcached use binary protocol for communication. It uses big endian byte order. All data are wrapped by command

```go
type Command struct {
	Code 		byte
	PayloadLen 	uint32
	Payload 	[]byte
}
```

*Code:* is enum (SET: 0x01, GET: 0x02, DELETE: 0x03, KEYS: 0x04)

All response from the server are wrapped by Result

```go
type Result struct {
	Code 		byte
	PayloadLen 	uint32
	Payload 	[]byte
}
```

*Code:* is enum (SUCCESS: 0xFF, ERROR: 0xFE, ERROR_NOT_FOUND: 0xFD)

- ERROR code always contains has not empty payload which is string in UTF8 encoding.
- ERROR_NOT_FOUND code always has empty payload

### Set key value

The operation store key-value in Tcached server. SET command code is used. Payload contains following binary structure (big endian byte order)

```go
type KeyValue struct {
	Expired  uint32
	ValueLen uint32
	KeyLen   uint16
	Key		 []byte
	Value	 []byte
}
```

Return SUCCESS response code with empty payload.

### Get value by key

The operation get stored value by key. GET command code is used. Payload contains key. Return SUCCESS response code with payload

```go
type KeyValue struct {
	Expired  uint32
	ValueLen uint32
	KeyLen   uint16
	Key		 []byte
	Value	 []byte
}
```

### Delete key

The operation delete stored value by key. DELETE command code is used. Payload contains key. Return SUCCESS response code with empty payload

### Keys

Get all stored keys. KEYS command code is used and payload is empty. Return SUCCESS response code with payload:

```go
type List struct {
	Count uint64
	Items []Item
}

type Item struct {
	Length uint16
	Value  []byte
}
```

*Value:* is string

## TODO

* Fix bug with OOM
* Store on disk
* Replication