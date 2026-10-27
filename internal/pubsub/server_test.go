package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"realtime-chat/internal/domain"
	"realtime-chat/internal/registry"
	"realtime-chat/internal/store"
)

type recorder struct {
	mu   sync.Mutex
	seqs []int64
}

func (r *recorder) Deliver(ev Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ev.Message != nil {
		r.seqs = append(r.seqs, ev.Message.Seq)
	}
	return nil
}

func (r *recorder) got() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.seqs...)
}

type gateways map[string]Subscriber

func (g gateways) Gateway(id string) (Subscriber, bool) {
	s, ok := g[id]
	return s, ok
}

type noNotify struct{}

func (noNotify) NotifyUser(context.Context, string, Event) error { return nil }

type cluster struct {
	t     *testing.T
	ctx   context.Context
	store *store.Store
	reg   *registry.Registry
	gws   gateways
}

func newCluster(t *testing.T, ttl time.Duration) *cluster {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "chat.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		time.Sleep(20 * time.Millisecond)
		st.Close()
	})
	reg := registry.New(1, ttl)
	go reg.Run(ctx)
	if err := st.CreateChat(ctx, domain.Chat{ID: "c1", Name: "team", CreatedBy: "a", CreatedAt: time.Now()}, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	return &cluster{t: t, ctx: ctx, store: st, reg: reg, gws: gateways{"gw": &recorder{}}}
}

func (c *cluster) start(id string) *Server {
	s := New(id, Config{MaxBatch: 64, HeartbeatEvery: 20 * time.Millisecond}, c.store, c.reg, c.gws, noNotify{}, slog.New(slog.DiscardHandler))
	s.Start(c.ctx)
	return s
}

// waitOwner 等到 shard 0 由 s 以 registry 上的 epoch 接手。
func (c *cluster) waitOwner(s *Server) {
	c.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		a := c.reg.Table().Owner(0)
		if a.Server == s.ID() && s.Holds(0, a.Epoch) {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("%s did not take over shard 0", s.ID())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func send(s *Server, sender, clientID string) (domain.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.Send(ctx, SendRequest{ChatID: "c1", SenderID: sender, ClientMsgID: clientID, Body: "hello"})
}

func TestOwnerSequencesDedupesAndPublishesInOrder(t *testing.T) {
	c := newCluster(t, time.Second)
	s := c.start("a")
	c.waitOwner(s)
	if err := s.Subscribe(domain.ChatTopic("c1"), "gw"); err != nil {
		t.Fatal(err)
	}

	for i, step := range []struct {
		sender, id string
		want       int64
	}{{"a", "a-1", 1}, {"b", "b-1", 2}, {"a", "a-1", 1}, {"a", "a-2", 3}} {
		m, err := send(s, step.sender, step.id)
		if err != nil || m.Seq != step.want {
			t.Fatalf("step %d: seq=%d err=%v", i, m.Seq, err)
		}
	}
	if _, err := send(s, "stranger", "x-1"); !errors.Is(err, domain.ErrNotMember) {
		t.Fatalf("non-member: err = %v", err)
	}

	ctx := context.Background()
	if m, err := s.Modify(ctx, ModifyRequest{ChatID: "c1", ActorID: "a", TargetID: "c", Op: domain.OpAdd, ClientMsgID: "a-3"}); err != nil || m.Seq != 4 {
		t.Fatalf("add: seq=%d err=%v", m.Seq, err)
	}
	if _, err := s.Modify(ctx, ModifyRequest{ChatID: "c1", ActorID: "a", TargetID: "c", Op: domain.OpAdd, ClientMsgID: "a-4"}); !errors.Is(err, domain.ErrAlreadyIn) {
		t.Fatalf("add twice: err = %v", err)
	}
	if m, err := s.Modify(ctx, ModifyRequest{ChatID: "c1", ActorID: "a", TargetID: "b", Op: domain.OpRemove, ClientMsgID: "a-5"}); err != nil || m.Seq != 5 {
		t.Fatalf("remove: seq=%d err=%v", m.Seq, err)
	}
	if _, err := send(s, "b", "b-2"); !errors.Is(err, domain.ErrNotMember) {
		t.Fatalf("removed member can still send: err = %v", err)
	}

	rec := c.gws["gw"].(*recorder)
	deadline := time.Now().Add(time.Second)
	for len(rec.got()) < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := rec.got(); fmt.Sprint(got) != "[1 2 3 4 5]" {
		t.Fatalf("published %v", got)
	}
}

func TestZombieOwnerIsFencedAndTheNewOwnerContinuesTheSequence(t *testing.T) {
	c := newCluster(t, 150*time.Millisecond)
	old := c.start("a")
	c.waitOwner(old)
	next := c.start("b") // 只有一個 shard，b 先待命
	if m, err := send(old, "a", "a-1"); err != nil || m.Seq != 1 {
		t.Fatalf("seq=%d err=%v", m.Seq, err)
	}

	// a 在寫入前停頓，租約過期，b 接手並立下新的 epoch。
	old.Pause(800 * time.Millisecond)
	_, err := send(old, "a", "a-2")
	if !errors.Is(err, domain.ErrNotOwner) {
		t.Fatalf("zombie write: err = %v", err)
	}
	if old.Stats().Fenced != 1 {
		t.Fatalf("fenced = %d", old.Stats().Fenced)
	}
	c.waitOwner(next)

	// client 重送同一則訊息到新 owner：接著原本的序號，沒有空號也沒有重複。
	if m, err := send(next, "a", "a-2"); err != nil || m.Seq != 2 {
		t.Fatalf("retry on new owner: seq=%d err=%v", m.Seq, err)
	}
	msgs, _ := c.store.Messages(context.Background(), "c1")
	if len(msgs) != 2 || msgs[1].ClientMsgID != "a-2" {
		t.Fatalf("stored %+v", msgs)
	}
}
