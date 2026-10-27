// Package pubsub 是以 chat 為單位分片的 pub/sub server。每個 shard 同時只有一個 owner：
// 它是該 shard 內所有 chat 的唯一寫入者，替訊息指派連續的 seq、寫入儲存後再推給訂閱的 gateway。
package pubsub

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"realtime-chat/internal/domain"
	"realtime-chat/internal/registry"
	"realtime-chat/internal/store"
)

// Event 是 owner 推給 gateway 的內容：chat 主題上是一則訊息，user 主題上是「你被加進了某個 chat」。
type Event struct {
	Topic     string
	Message   *domain.Message
	ChatAdded *domain.ChatState
}

// Subscriber 是 gateway 接收推播的入口。Deliver 不能阻塞，owner 是在自己的推播佇列裡依序呼叫它。
type Subscriber interface {
	Deliver(ev Event) error
}

// Gateways 依 id 找到 gateway；找不到代表 gateway 已經下線。
type Gateways interface {
	Gateway(id string) (Subscriber, bool)
}

// Notifier 把事件發布到 user 主題。user 主題可能由另一台 server 負責，所以透過 router 轉送。
type Notifier interface {
	NotifyUser(ctx context.Context, userID string, ev Event) error
}

type Config struct {
	// MaxBatch 是一次交易最多寫入幾個請求。批次越大，平均每則訊息分攤到的 fsync 越少。
	MaxBatch       int
	HeartbeatEvery time.Duration
}

func DefaultConfig() Config {
	return Config{MaxBatch: 256, HeartbeatEvery: 100 * time.Millisecond}
}

type Stats struct {
	Batches    int64
	Committed  int64
	Duplicates int64
	Fenced     int64
	Pushes     int64
}

type Server struct {
	id     string
	cfg    Config
	store  *store.Store
	reg    *registry.Registry
	gws    Gateways
	notify Notifier
	log    *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	reqs   chan *request

	mu          sync.Mutex
	pausedUntil time.Time
	owned       map[int]int64
	subs        map[string]map[string]struct{}
	next        map[string]int64
	outs        map[string]chan Event

	batches, committed, duplicates, fenced, pushes atomic.Int64
}

func New(id string, cfg Config, st *store.Store, reg *registry.Registry, gws Gateways, notify Notifier, log *slog.Logger) *Server {
	return &Server{id: id, cfg: cfg, store: st, reg: reg, gws: gws, notify: notify, log: log,
		done: make(chan struct{}), reqs: make(chan *request, 4096),
		owned: make(map[int]int64), subs: make(map[string]map[string]struct{}),
		next: make(map[string]int64), outs: make(map[string]chan Event)}
}

func (s *Server) ID() string { return s.id }

// Start 向 registry 註冊並開始服務，直到 ctx 結束或 Kill。
func (s *Server) Start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)
	watch := s.reg.Watch()
	s.reg.Register(s.id)
	var wg sync.WaitGroup
	for _, fn := range []func(){s.writeLoop, s.heartbeatLoop, func() { s.watchLoop(watch) }} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}
	go func() {
		wg.Wait()
		close(s.done)
	}()
}

// Kill 模擬整台 server 當掉：不再處理請求、不再送 heartbeat，租約過期後 shard 由其他 server 接手。
func (s *Server) Kill() {
	s.cancel()
	<-s.done
}

// Pause 模擬 stop-the-world 的停頓（例如長時間 GC）：期間不送 heartbeat，
// 手上已經組好的那一批寫入也停在半路，醒來後才繼續。
func (s *Server) Pause(d time.Duration) {
	s.mu.Lock()
	s.pausedUntil = time.Now().Add(d)
	s.mu.Unlock()
}

func (s *Server) waitIfPaused() {
	s.mu.Lock()
	until := s.pausedUntil
	s.mu.Unlock()
	if d := time.Until(until); d > 0 {
		select {
		case <-time.After(d):
		case <-s.ctx.Done():
		}
	}
}

func (s *Server) Stats() Stats {
	return Stats{Batches: s.batches.Load(), Committed: s.committed.Load(), Duplicates: s.duplicates.Load(),
		Fenced: s.fenced.Load(), Pushes: s.pushes.Load()}
}

func (s *Server) alive() bool {
	return s.ctx != nil && s.ctx.Err() == nil
}

func (s *Server) shardOf(topic string) int {
	return registry.ShardOf(topic, s.reg.Shards())
}

func (s *Server) owns(topic string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.owned[s.shardOf(topic)]
	return ok
}

func (s *Server) heartbeatLoop() {
	ticker := time.NewTicker(s.cfg.HeartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		s.waitIfPaused()
		if s.ctx.Err() != nil {
			return
		}
		if !s.reg.Heartbeat(s.id) {
			// 租約已經過期，shard 都交給別人了。先放掉自己以為還擁有的 shard，再以新成員的身分加入。
			s.log.Warn("lease lost, rejoining", "server", s.id)
			s.mu.Lock()
			for shard := range s.owned {
				s.dropShardLocked(shard)
			}
			s.mu.Unlock()
			s.reg.Register(s.id)
		}
	}
}

func (s *Server) watchLoop(watch <-chan registry.Table) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case t := <-watch:
			s.waitIfPaused()
			s.applyTable(t)
		}
	}
}

// applyTable 依最新的對應表接手或放掉 shard。接手前先在儲存層用新的 epoch 立下 fence，
// 之後舊 owner 就算還活著也寫不進去；接手後讀到的最新 seq 因此不會再被別人改變。
func (s *Server) applyTable(t registry.Table) {
	s.mu.Lock()
	var claims []int
	for shard, a := range t.Shards {
		epoch, owned := s.owned[shard]
		switch {
		case a.Server == s.id && (!owned || epoch != a.Epoch):
			claims = append(claims, shard)
		case a.Server != s.id && owned:
			s.dropShardLocked(shard)
		}
	}
	s.mu.Unlock()
	if len(claims) == 0 {
		return
	}

	var won []int
	err := s.store.Write(s.ctx, func(tx *store.Tx) error {
		won = won[:0]
		for _, shard := range claims {
			err := tx.Fence(shard, t.Shards[shard].Epoch)
			if errors.Is(err, domain.ErrFenced) {
				continue
			}
			if err != nil {
				return err
			}
			won = append(won, shard)
		}
		return nil
	})
	if err != nil {
		s.log.Error("claim shards", "server", s.id, "err", err)
		return
	}
	s.mu.Lock()
	for _, shard := range won {
		s.dropShardLocked(shard)
		s.owned[shard] = t.Shards[shard].Epoch
	}
	s.mu.Unlock()
}

// dropShardLocked 清掉 shard 的所有本機狀態：訂閱與 seq 快取都只在 owner 身上有效。
func (s *Server) dropShardLocked(shard int) {
	delete(s.owned, shard)
	for topic := range s.subs {
		if s.shardOf(topic) == shard {
			delete(s.subs, topic)
		}
	}
	for chat := range s.next {
		if s.shardOf(domain.ChatTopic(chat)) == shard {
			delete(s.next, chat)
		}
	}
}

// Subscribe 讓 gateway 訂閱 topic。gateway 必須先訂閱、再補歷史訊息，兩者之間才不會有空窗。
func (s *Server) Subscribe(topic, gatewayID string) error {
	if !s.alive() {
		return domain.ErrUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.owned[s.shardOf(topic)]; !ok {
		return domain.ErrNotOwner
	}
	if s.subs[topic] == nil {
		s.subs[topic] = make(map[string]struct{})
	}
	s.subs[topic][gatewayID] = struct{}{}
	s.outboxLocked(gatewayID)
	return nil
}

func (s *Server) Unsubscribe(topic, gatewayID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs[topic], gatewayID)
}

// PublishUser 把事件推給訂閱 user 主題的 gateway。
func (s *Server) PublishUser(userID string, ev Event) error {
	if !s.alive() {
		return domain.ErrUnavailable
	}
	ev.Topic = domain.UserTopic(userID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.owned[s.shardOf(ev.Topic)]; !ok {
		return domain.ErrNotOwner
	}
	s.publishLocked(ev)
	return nil
}

func (s *Server) publishLocked(ev Event) {
	for gw := range s.subs[ev.Topic] {
		s.outboxLocked(gw) <- ev
	}
}

// outboxLocked 回傳 gateway 的推播佇列。每個 gateway 一條佇列、一個 goroutine 依序送出，
// 同一個 chat 的訊息到 gateway 的順序就是 commit 的順序。
func (s *Server) outboxLocked(gatewayID string) chan Event {
	if ch, ok := s.outs[gatewayID]; ok {
		return ch
	}
	ch := make(chan Event, 1<<16)
	s.outs[gatewayID] = ch
	go s.pushLoop(gatewayID, ch)
	return ch
}

func (s *Server) pushLoop(gatewayID string, ch chan Event) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case ev := <-ch:
			gw, ok := s.gws.Gateway(gatewayID)
			if ok && gw.Deliver(ev) == nil {
				s.pushes.Add(1)
				continue
			}
			// gateway 下線了：清掉它的所有訂閱，它的使用者會重新連線到別的 gateway 並補訊息。
			s.mu.Lock()
			for topic := range s.subs {
				delete(s.subs[topic], gatewayID)
			}
			delete(s.outs, gatewayID)
			s.mu.Unlock()
			return
		}
	}
}

// Holds 回傳是否已經以指定的 epoch 接手 shard。
func (s *Server) Holds(shard int, epoch int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.owned[shard]
	return ok && e == epoch
}
