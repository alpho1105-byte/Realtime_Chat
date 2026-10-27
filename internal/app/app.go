package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"realtime-chat/internal/chats"
	"realtime-chat/internal/cluster"
	"realtime-chat/internal/gateway"
	"realtime-chat/internal/pubsub"
	"realtime-chat/internal/registry"
	"realtime-chat/internal/store"
)

type Options struct {
	DBPath        string
	Shards        int
	PubSubServers int
	Gateways      int
	// LeaseTTL：pub/sub server 停止 heartbeat 多久之後，它的 shard 交給別人。
	LeaseTTL time.Duration
	PubSub   pubsub.Config
	Gateway  gateway.Config
	Logger   *slog.Logger
}

func DefaultOptions(dbPath string) Options {
	return Options{DBPath: dbPath, Shards: 64, PubSubServers: 4, Gateways: 4, LeaseTTL: 500 * time.Millisecond,
		PubSub: pubsub.DefaultConfig(), Gateway: gateway.DefaultConfig(), Logger: slog.Default()}
}

// App 組裝所有元件。正式環境裡它們是各自獨立部署的服務，這裡放在同一個 process，
// 彼此只透過 cluster.Network 與 registry 找到對方。
type App struct {
	opts     Options
	Store    *store.Store
	Registry *registry.Registry
	Network  *cluster.Network
	Router   *cluster.Router
	Chats    *chats.Service

	ctx      context.Context
	mu       sync.Mutex
	servers  []*pubsub.Server
	gateways []*gateway.Gateway
	nextGW   int
}

func New(opts Options) (*App, error) {
	st, err := store.Open(opts.DBPath)
	if err != nil {
		return nil, err
	}
	a := &App{opts: opts, Store: st, Registry: registry.New(opts.Shards, opts.LeaseTTL), Network: cluster.NewNetwork()}
	a.Router = cluster.NewRouter(a.Registry, a.Network)
	a.Chats = chats.NewService(st, a.Router, opts.Logger)
	return a, nil
}

// Start 啟動 registry、pub/sub server 與 gateway，等到每個 shard 都有 owner 才回傳。
func (a *App) Start(ctx context.Context) error {
	a.ctx = ctx
	go a.Registry.Run(ctx)
	for i := range a.opts.PubSubServers {
		s := pubsub.New(fmt.Sprintf("pubsub-%d", i), a.opts.PubSub, a.Store, a.Registry, a.Network, a.Router,
			a.opts.Logger.With("component", "pubsub"))
		a.Network.AddServer(s)
		s.Start(ctx)
		a.servers = append(a.servers, s)
	}
	for range a.opts.Gateways {
		a.AddGateway()
	}
	deadline := time.Now().Add(10 * time.Second)
	for !a.Ready() {
		if time.Now().After(deadline) {
			return errors.New("shards were not claimed in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// Ready 回傳每個 shard 是否都已經被 registry 指定的 owner 接手。
func (a *App) Ready() bool {
	for shard, as := range a.Registry.Table().Shards {
		s, ok := a.Network.Server(as.Server)
		if !ok || !s.Holds(shard, as.Epoch) {
			return false
		}
	}
	return true
}

// AddGateway 啟動一台新的 gateway，例如替換掉當掉的那台。
func (a *App) AddGateway() *gateway.Gateway {
	a.mu.Lock()
	defer a.mu.Unlock()
	g := gateway.New(fmt.Sprintf("gateway-%d", a.nextGW), a.opts.Gateway, a.Router, a.Network, a.Registry, a.Store, a.Chats,
		a.opts.Logger.With("component", "gateway"))
	a.nextGW++
	g.Start(a.ctx)
	a.gateways = append(a.gateways, g)
	return g
}

func (a *App) Servers() []*pubsub.Server {
	return a.servers
}

func (a *App) Gateways() []*gateway.Gateway {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]*gateway.Gateway(nil), a.gateways...)
}

// KillGateway 讓一台 gateway 當掉，並把它從負載平衡器的名單中移除。
func (a *App) KillGateway(g *gateway.Gateway) {
	a.mu.Lock()
	for i, x := range a.gateways {
		if x == g {
			a.gateways = append(a.gateways[:i], a.gateways[i+1:]...)
			break
		}
	}
	a.mu.Unlock()
	g.Kill()
}

// Connect 扮演負載平衡器：把新連線隨機分到一台存活的 gateway。
func (a *App) Connect(ctx context.Context, userID string) (*gateway.Session, error) {
	gws := a.Gateways()
	if len(gws) == 0 {
		return nil, errors.New("no gateway available")
	}
	return gws[rand.IntN(len(gws))].Connect(ctx, userID)
}

func (a *App) Close() error {
	return a.Store.Close()
}
