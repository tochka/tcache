package client_test

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochka/tcached/client"
)

var (
	c *client.Client
)

func TestMain(m *testing.M) {
	c = client.NewClient("127.0.0.1:30003")
	rand.Seed(time.Now().UnixNano())

	os.Exit(m.Run())
}

func TestClientAdd(t *testing.T) {
	var table = []struct {
		name      string
		makeValue func(t *testing.T) client.Value
	}{
		{
			name: "simple case",
			makeValue: func(t *testing.T) client.Value {
				return client.Value{Expired: uint32(time.Now().Add(1 * time.Hour).UTC().Unix()), Value: []byte("test")}
			},
		},
		{
			name: "add expired value",
			makeValue: func(t *testing.T) client.Value {
				return client.Value{Expired: uint32(time.Now().Add(-1 * time.Hour).UTC().Unix()), Value: []byte("test")}
			},
		},
	}
	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			key := strconv.FormatInt(time.Now().UnixNano(), 10)
			err := c.Add(key, test.makeValue(t))
			assert.NoError(t, err)
		})
	}

}

func TestClientGet(t *testing.T) {
	val := generate(10)

	var table = []struct {
		name          string
		makeKey       func(t *testing.T) string
		expectedErr   error
		expectedValue []byte
	}{
		{
			name: "get value",
			makeKey: func(t *testing.T) string {
				key := strconv.FormatInt(time.Now().UnixNano(), 10)

				err := c.Add(key, client.Value{
					Expired: uint32(time.Now().Add(1 * time.Hour).UTC().Unix()),
					Value:   val,
				})
				require.NoError(t, err)
				return key
			},
			expectedErr:   nil,
			expectedValue: val,
		},
		{
			name: "key was not found",
			makeKey: func(t *testing.T) string {
				return strconv.FormatInt(time.Now().UnixNano(), 10)
			},
			expectedErr:   client.ErrEntityNotFound,
			expectedValue: nil,
		},
		{
			name: "key expired",
			makeKey: func(t *testing.T) string {
				key := strconv.FormatInt(time.Now().UnixNano(), 10)

				err := c.Add(key, client.Value{
					Expired: uint32(time.Now().Add(1 * time.Second).UTC().Unix()),
					Value:   val,
				})
				require.NoError(t, err)
				time.Sleep(2 * time.Second)
				return key
			},
			expectedErr:   client.ErrEntityNotFound,
			expectedValue: nil,
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			key := test.makeKey(t)
			v, err := c.Get(key)
			if test.expectedErr != nil {
				assert.Equal(t, test.expectedErr, err)
			}
			if len(test.expectedValue) != 0 {
				assert.Equal(t, test.expectedValue, v.Value)
			}
		})
	}
}

func TestClientDel(t *testing.T) {
	val := generate(10)

	var table = []struct {
		name        string
		makeKey     func(t *testing.T) string
		expectedErr error
	}{
		{
			name: "delete key which not exist",
			makeKey: func(t *testing.T) string {
				return strconv.FormatInt(time.Now().UnixNano(), 10)
			},
			expectedErr: nil,
		},
		{
			name: "delete key which was stored",
			makeKey: func(t *testing.T) string {
				key := strconv.FormatInt(time.Now().UnixNano(), 10)

				err := c.Add(key, client.Value{
					Expired: uint32(time.Now().Add(1 * time.Hour).UTC().Unix()),
					Value:   val,
				})
				require.NoError(t, err)
				return key
			},
			expectedErr: nil,
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			key := test.makeKey(t)
			err := c.Remove(key)
			if test.expectedErr != nil {
				assert.Equal(t, test.expectedErr, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestClientGetKeys(t *testing.T) {
	val := generate(10)

	expectedKey := t.Name() + strconv.FormatInt(time.Now().UnixNano(), 10)

	err := c.Add(expectedKey, client.Value{
		Expired: uint32(time.Now().Add(1 * time.Hour).UTC().Unix()),
		Value:   val,
	})
	require.NoError(t, err)

	keys, err := c.Keys()
	require.NoError(t, err)

	for _, key := range keys {
		if key == expectedKey {
			return
		}
	}
	t.Fatalf("%v key was not found", expectedKey)
}

func generate(len int) []byte {
	data := make([]byte, len)
	_, err := rand.Read(data)
	if err != nil {
		panic(err)
	}
	return data
}
