// Package cluster 是元件之間的「網路」：依 id 找到 pub/sub server 與 gateway，
// 並依 registry 的對應表把請求送到 topic 的 owner。
package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	"realtime-chat/internal/domain"
	"realtime-chat/internal/pubsub"
	"realtime-chat/internal/registry"
)

type Network struct {
	mu       sync.RWMutex
	servers  map[string]*pubsub.Server
	gateways map[string]pubsub.Subscriber
}

func NewNetwork() *Network {
	return &Network{servers: make(map[string]*pubsub.Server), gateways: make(map[string]pubsub.Subscriber)}
}

func (n *Network) AddServer(s *pubsub.Server) {
	n.mu.Lock()
	n.servers[s.ID()] = s
	n.mu.Unlock()
}

func (n *Network) Server(id string) (*pubsub.Server, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	s, ok := n.servers[id]
	return s, ok
}

func (n *Network) AddGateway(id string, g pubsub.Subscriber) {
	n.mu.Lock()
	n.gateways[id] = g
	n.mu.Unlock()
}

func (n *Network) RemoveGateway(id string) {
	n.mu.Lock()
	delete(n.gateways, id)
	n.mu.Unlock()
}

func (n *Network) Gateway(id string) (pubsub.Subscriber, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	g, ok := n.gateways[id]
	return g, ok
}

type Router struct {
	reg   *registry.Registry
	net   *Network
	retry time.Duration
}

func NewRouter(reg *registry.Registry, net *Network) *Router {
	return &Router{reg: reg, net: net, retry: 20 * time.Millisecond}
}

// Do 對 topic 目前的 owner 執行 fn。owner 剛掛掉或剛換人時會回 ErrUnavailable / ErrNotOwner，
// 這時等一下、重新查對應表再試，直到 ctx 結束。
func (r *Router) Do(ctx context.Context, topic string, fn func(*pubsub.Server) error) error {
	for {
		err := domain.ErrUnavailable
		a := r.reg.Table().Owner(registry.ShardOf(topic, r.reg.Shards()))
		if s, ok := r.net.Server(a.Server); ok {
			err = fn(s)
		}
		if !errors.Is(err, domain.ErrNotOwner) && !errors.Is(err, domain.ErrUnavailable) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(r.retry):
		}
	}
}

func (r *Router) NotifyUser(ctx context.Context, userID string, ev pubsub.Event) error {
	return r.Do(ctx, domain.UserTopic(userID), func(s *pubsub.Server) error { return s.PublishUser(userID, ev) })
}
