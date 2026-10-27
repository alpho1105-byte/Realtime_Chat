// Package gateway 維持使用者的長連線。gateway 本身不保存聊天狀態：它向 chat 的 owner 訂閱，
// 收到推播後轉給本機上屬於該 chat 的連線。使用者可以連到任何一台 gateway。
package gateway

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"realtime-chat/internal/chats"
	"realtime-chat/internal/cluster"
	"realtime-chat/internal/domain"
	"realtime-chat/internal/protocol"
	"realtime-chat/internal/pubsub"
	"realtime-chat/internal/registry"
	"realtime-chat/internal/store"
)

type Config struct {
	// SessionBuffer 是每條連線的推播佇列長度；塞滿代表 client 跟不上，直接斷線讓它重連後補訊息。
	SessionBuffer  int
	RequestTimeout time.Duration
	AckFlushEvery  time.Duration
	SyncLimit      int
}

func DefaultConfig() Config {
	return Config{SessionBuffer: 4096, RequestTimeout: 5 * time.Second, AckFlushEvery: 100 * time.Millisecond, SyncLimit: 500}
}

type ackKey struct{ chat, user string }

type Gateway struct {
	id     string
	cfg    Config
	router *cluster.Router
	net    *cluster.Network
	reg    *registry.Registry
	store  *store.Store
	chats  *chats.Service
	log    *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu       sync.Mutex
	sessions map[string]map[*Session]struct{}
	// interest：topic → 本機上需要它的使用者。chat 主題是目前的成員，user 主題是使用者自己。
	interest map[string]map[string]struct{}
	acks     map[ackKey]int64
}

func New(id string, cfg Config, router *cluster.Router, net *cluster.Network, reg *registry.Registry, st *store.Store, cs *chats.Service, log *slog.Logger) *Gateway {
	return &Gateway{id: id, cfg: cfg, router: router, net: net, reg: reg, store: st, chats: cs, log: log,
		done: make(chan struct{}), sessions: make(map[string]map[*Session]struct{}),
		interest: make(map[string]map[string]struct{}), acks: make(map[ackKey]int64)}
}

func (g *Gateway) ID() string { return g.id }

func (g *Gateway) Start(ctx context.Context) {
	g.ctx, g.cancel = context.WithCancel(ctx)
	g.net.AddGateway(g.id, g)
	watch := g.reg.Watch()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		g.watchLoop(watch)
	}()
	go func() {
		defer wg.Done()
		g.ackLoop()
	}()
	go func() {
		wg.Wait()
		close(g.done)
	}()
}

// Kill 模擬 gateway 當掉：所有連線中斷，client 會重新連到別台 gateway。
func (g *Gateway) Kill() {
	g.net.RemoveGateway(g.id)
	g.cancel()
	<-g.done
	g.mu.Lock()
	var all []*Session
	for _, set := range g.sessions {
		for s := range set {
			all = append(all, s)
		}
	}
	g.mu.Unlock()
	for _, s := range all {
		s.Close()
	}
}

// Connect 建立一條使用者連線：先訂閱使用者會用到的 topic，再送出 chat 清單讓 client 自己補訊息。
// 順序不能反過來，否則「補完歷史」和「開始收推播」之間的訊息就漏掉了。
func (g *Gateway) Connect(ctx context.Context, userID string) (*Session, error) {
	if g.ctx.Err() != nil {
		return nil, domain.ErrUnavailable
	}
	s := &Session{gw: g, user: userID, out: make(chan protocol.ServerFrame, g.cfg.SessionBuffer)}
	g.mu.Lock()
	if g.sessions[userID] == nil {
		g.sessions[userID] = make(map[*Session]struct{})
	}
	g.sessions[userID][s] = struct{}{}
	g.mu.Unlock()

	if err := g.refreshUser(ctx, userID); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// refreshUser 依儲存層的成員資料重建這位使用者的訂閱，然後送出 chat 清單。
//
// 先訂閱、再確認成員資格：讀取成員名單和訂閱生效之間，可能剛好有人把他移出聊天室。
// 移除發生在訂閱之後，gateway 會收到那則異動；發生在訂閱之前，第二次讀取就看得到。
func (g *Gateway) refreshUser(ctx context.Context, userID string) error {
	if err := g.subscribe(ctx, domain.UserTopic(userID), userID); err != nil {
		return err
	}
	states, err := g.store.ChatsOf(ctx, userID)
	if err != nil {
		return err
	}
	for _, st := range states {
		if st.Member {
			if err := g.subscribe(ctx, domain.ChatTopic(st.ChatID), userID); err != nil {
				return err
			}
		}
	}
	if states, err = g.store.ChatsOf(ctx, userID); err != nil {
		return err
	}
	list := make([]protocol.ChatInfo, 0, len(states))
	for _, st := range states {
		if !st.Member {
			g.removeInterest(domain.ChatTopic(st.ChatID), userID)
		}
		list = append(list, protocol.ChatInfo{ChatID: st.ChatID, Name: st.Name, Member: st.Member, LastAck: st.LastAck})
	}
	g.pushUser(userID, protocol.ServerFrame{Type: protocol.TypeChatList, Chats: list})
	return nil
}

// dropIfRemoved 在訂閱之後確認使用者仍是成員；已經被移除就不再把這個 chat 的推播轉給他。
func (g *Gateway) dropIfRemoved(ctx context.Context, chatID, userID string) {
	if member, err := g.store.IsMember(ctx, chatID, userID); err == nil && !member {
		g.removeInterest(domain.ChatTopic(chatID), userID)
	}
}

func (g *Gateway) subscribe(ctx context.Context, topic, userID string) error {
	g.mu.Lock()
	if g.interest[topic] == nil {
		g.interest[topic] = make(map[string]struct{})
	}
	g.interest[topic][userID] = struct{}{}
	g.mu.Unlock()
	return g.router.Do(ctx, topic, func(s *pubsub.Server) error { return s.Subscribe(topic, g.id) })
}

func (g *Gateway) removeInterest(topic, userID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removeInterestLocked(topic, userID)
}

func (g *Gateway) removeInterestLocked(topic, userID string) {
	users, ok := g.interest[topic]
	if !ok {
		return
	}
	delete(users, userID)
	if len(users) > 0 {
		return
	}
	delete(g.interest, topic)
	// 盡力而為：就算沒取消成功，之後收到的推播也只是被忽略，owner 換人時訂閱也會被清掉。
	a := g.reg.Table().Owner(registry.ShardOf(topic, g.reg.Shards()))
	if s, ok := g.net.Server(a.Server); ok {
		s.Unsubscribe(topic, g.id)
	}
}

// Deliver 接收 owner 的推播，轉給本機上需要它的連線。不能阻塞：owner 依序對每個 gateway 推播。
func (g *Gateway) Deliver(ev pubsub.Event) error {
	if g.ctx.Err() != nil {
		return domain.ErrUnavailable
	}
	switch {
	case ev.Message != nil:
		m := ev.Message
		frame := protocol.ServerFrame{Type: protocol.TypeNewMessage, ChatID: m.ChatID, Message: ptr(protocol.FromMessage(*m))}
		g.mu.Lock()
		for user := range g.interest[ev.Topic] {
			g.pushUserLocked(user, frame)
		}
		if m.Kind == domain.KindMemberRemoved {
			// 被移除的人收得到「自己被移除」這則，之後的訊息就不再轉給他。
			g.removeInterestLocked(ev.Topic, m.Target)
		}
		g.mu.Unlock()
	case ev.ChatAdded != nil:
		user := strings.TrimPrefix(ev.Topic, "user:")
		g.mu.Lock()
		_, local := g.sessions[user]
		g.mu.Unlock()
		if local {
			go g.onChatAdded(user, *ev.ChatAdded)
		}
	}
	return nil
}

// onChatAdded 先訂閱新的 chat，再通知 client，client 收到後從加入的位置開始補訊息。
func (g *Gateway) onChatAdded(userID string, st domain.ChatState) {
	ctx, cancel := context.WithTimeout(g.ctx, g.cfg.RequestTimeout)
	defer cancel()
	if err := g.subscribe(ctx, domain.ChatTopic(st.ChatID), userID); err != nil {
		g.log.Warn("subscribe new chat", "gateway", g.id, "chat", st.ChatID, "err", err)
		return
	}
	// 通知可能晚到：他也許已經又被移出去了。
	g.dropIfRemoved(ctx, st.ChatID, userID)
	g.pushUser(userID, protocol.ServerFrame{Type: protocol.TypeChatAdded, ChatID: st.ChatID,
		Chats: []protocol.ChatInfo{{ChatID: st.ChatID, Name: st.Name, Member: true}}})
}

func (g *Gateway) pushUser(userID string, f protocol.ServerFrame) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pushUserLocked(userID, f)
}

func (g *Gateway) pushUserLocked(userID string, f protocol.ServerFrame) {
	for s := range g.sessions[userID] {
		s.push(f)
	}
}

// watchLoop 在 shard 換 owner 時，把受影響的 topic 改向新 owner 訂閱。
// 舊 owner 掛掉到新 owner 接手之間的推播可能已經遺失，所以訂閱完成後請 client 重新同步。
func (g *Gateway) watchLoop(watch <-chan registry.Table) {
	var last registry.Table
	for {
		var t registry.Table
		select {
		case <-g.ctx.Done():
			return
		case t = <-watch:
		}
		if last.Shards == nil {
			last = t
			continue
		}
		moved := make(map[int]bool)
		for i, a := range t.Shards {
			if a != last.Shards[i] {
				moved[i] = true
			}
		}
		last = t

		g.mu.Lock()
		affected := make(map[string][]string)
		for topic, users := range g.interest {
			if moved[registry.ShardOf(topic, len(t.Shards))] {
				for u := range users {
					affected[topic] = append(affected[topic], u)
				}
			}
		}
		g.mu.Unlock()

		for topic, users := range affected {
			ctx, cancel := context.WithTimeout(g.ctx, g.cfg.RequestTimeout)
			err := g.router.Do(ctx, topic, func(s *pubsub.Server) error { return s.Subscribe(topic, g.id) })
			cancel()
			if err != nil {
				g.log.Warn("resubscribe", "gateway", g.id, "topic", topic, "err", err)
				continue
			}
			if chatID, ok := strings.CutPrefix(topic, "chat:"); ok {
				// 遺失的推播裡可能有「某人被移除」，gateway 沒看到就會繼續轉訊息給他。
				ctx, cancel := context.WithTimeout(g.ctx, g.cfg.RequestTimeout)
				for _, u := range users {
					g.dropIfRemoved(ctx, chatID, u)
					g.pushUser(u, protocol.ServerFrame{Type: protocol.TypeResync, ChatID: chatID})
				}
				cancel()
			} else {
				// user 主題換 owner：中間可能漏掉「被加入新 chat」的通知，依儲存層重建一次。
				ctx, cancel := context.WithTimeout(g.ctx, g.cfg.RequestTimeout)
				if err := g.refreshUser(ctx, users[0]); err != nil {
					g.log.Warn("refresh user", "gateway", g.id, "user", users[0], "err", err)
				}
				cancel()
			}
		}
	}
}

func (g *Gateway) queueAck(chatID, userID string, seq int64) {
	g.mu.Lock()
	k := ackKey{chatID, userID}
	g.acks[k] = max(g.acks[k], seq)
	g.mu.Unlock()
}

// ackLoop 把已讀位置合併後批次寫入：每個 client 每秒都可能送好幾次 ack，不值得各自一次交易。
func (g *Gateway) ackLoop() {
	ticker := time.NewTicker(g.cfg.AckFlushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
		}
		g.mu.Lock()
		pending := g.acks
		g.acks = make(map[ackKey]int64)
		g.mu.Unlock()
		if len(pending) == 0 {
			continue
		}
		batch := make([]store.Ack, 0, len(pending))
		for k, seq := range pending {
			batch = append(batch, store.Ack{ChatID: k.chat, UserID: k.user, Seq: seq})
		}
		if err := g.store.SaveAcks(g.ctx, batch); err != nil && g.ctx.Err() == nil {
			g.log.Error("save acks", "gateway", g.id, "err", err)
		}
	}
}

func (g *Gateway) detach(s *Session) {
	g.mu.Lock()
	defer g.mu.Unlock()
	set := g.sessions[s.user]
	delete(set, s)
	if len(set) > 0 {
		return
	}
	delete(g.sessions, s.user)
	for topic, users := range g.interest {
		if _, ok := users[s.user]; ok {
			g.removeInterestLocked(topic, s.user)
		}
	}
}

func ptr[T any](v T) *T { return &v }
