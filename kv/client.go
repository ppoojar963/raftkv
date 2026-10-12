package kv

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Server is anything that can execute an Op: an in-process *Store, or an HTTP stub.
type Server interface {
	Submit(Op) (Result, error)
}

// Client finds the leader and retries until an operation succeeds. A retry reuses
// the same (ClientID, Seq), which is what makes retried writes exactly-once.
// A Client issues one operation at a time; use one Client per goroutine.
type Client struct {
	id      string
	seq     atomic.Int64
	n       int
	server  func(i int) Server // looked up per attempt, so restarted replicas are picked up
	leader  int
	Timeout time.Duration
}

func NewClient(n int, server func(i int) Server) *Client {
	b := make([]byte, 8)
	rand.Read(b)
	return &Client{id: hex.EncodeToString(b), n: n, server: server, Timeout: 30 * time.Second}
}

func (c *Client) ID() string { return c.id }

func (c *Client) Get(key string) (string, bool, error) {
	r, err := c.do(Op{Type: OpGet, Key: key})
	return r.Value, r.Found, err
}

func (c *Client) Put(key, value string) error {
	_, err := c.do(Op{Type: OpPut, Key: key, Value: value})
	return err
}

func (c *Client) Delete(key string) error {
	_, err := c.do(Op{Type: OpDelete, Key: key})
	return err
}

func (c *Client) do(op Op) (Result, error) {
	op.ClientID = c.id
	op.Seq = c.seq.Add(1)
	deadline := time.Now().Add(c.Timeout)
	tries := 0
	suspect := -1 // a server that just timed out; followers may still point at it
	for time.Now().Before(deadline) {
		res, err := c.server(c.leader).Submit(op)
		if err == nil {
			return res, nil
		}
		var nl *NotLeaderError
		switch {
		case errors.As(err, &nl) && nl.LeaderID >= 0 && nl.LeaderID != c.leader && nl.LeaderID != suspect:
			c.leader = nl.LeaderID
		default:
			if !errors.As(err, &nl) {
				suspect = c.leader // timed out or stopped: likely partitioned or crashed
			}
			c.leader = (c.leader + 1) % c.n
		}
		if tries++; tries%c.n == 0 {
			time.Sleep(20 * time.Millisecond) // full sweep failed; back off
		}
	}
	return Result{}, fmt.Errorf("kv: %s %q gave up after %v", op.Type, op.Key, c.Timeout)
}
