package main

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"realtime-chat/internal/gateway"
	"realtime-chat/internal/protocol"
)

// lossyConn 模擬不穩定的行動網路：有些 sendMessage 請求根本沒送到，
// 有些送到了、server 也寫入了，但回應在路上遺失，client 只能逾時後重送。
type lossyConn struct {
	sess *gateway.Session
	out  chan protocol.ServerFrame
	net  *netStats

	mu       sync.Mutex
	dropResp map[string]bool
}

type netStats struct {
	lostRequests  atomic.Int64
	lostResponses atomic.Int64
}

const (
	requestLoss  = 0.01
	responseLoss = 0.01
)

func newLossyConn(sess *gateway.Session, net *netStats) *lossyConn {
	c := &lossyConn{sess: sess, out: make(chan protocol.ServerFrame, 256), net: net, dropResp: make(map[string]bool)}
	go func() {
		defer close(c.out)
		for f := range sess.Frames() {
			if f.Type == protocol.TypeResponse {
				c.mu.Lock()
				drop := c.dropResp[f.ReqID]
				delete(c.dropResp, f.ReqID)
				c.mu.Unlock()
				if drop {
					c.net.lostResponses.Add(1)
					continue
				}
			}
			c.out <- f
		}
	}()
	return c
}

func (c *lossyConn) Send(f protocol.ClientFrame) error {
	if f.Type == protocol.TypeSend {
		switch r := rand.Float64(); {
		case r < requestLoss:
			c.net.lostRequests.Add(1)
			return nil
		case r < requestLoss+responseLoss:
			c.mu.Lock()
			c.dropResp[f.ReqID] = true
			c.mu.Unlock()
		}
	}
	return c.sess.Send(f)
}

func (c *lossyConn) Frames() <-chan protocol.ServerFrame { return c.out }
func (c *lossyConn) Close()                              { c.sess.Close() }
