// simulate 讓 5,000 位使用者在 1,500 個聊天室裡聊天 20 秒。網路會遺失請求與回應、
// 部分使用者中途離線、成員持續被加入與移除，途中再讓一台 pub/sub server 當掉、
// 一台停頓到租約過期（醒來後成為 zombie owner）、一台 gateway 當掉。
// 最後以儲存層的日誌為準，驗證每位使用者看到的訊息完全一致、沒有遺失也沒有重複。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"realtime-chat/internal/app"
	"realtime-chat/internal/client"
	"realtime-chat/internal/domain"
	"realtime-chat/internal/metrics"
	"realtime-chat/internal/protocol"
)

const (
	trafficFor   = 20 * time.Second
	sendInterval = 2 * time.Second
	offlineRate  = 0.10
	churnEvery   = 100 * time.Millisecond
	faultWindow  = 2 * time.Second
)

func main() {
	seed := flag.Uint64("seed", 42, "random seed")
	verbose := flag.Bool("v", false, "print component logs")
	flag.Parse()

	level := slog.LevelError + 4
	if *verbose {
		level = slog.LevelInfo
	}
	if err := run(*seed, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))); err != nil {
		fmt.Fprintln(os.Stderr, "simulation failed:", err)
		os.Exit(1)
	}
}

func userID(i int) string { return fmt.Sprintf("u%04d", i) }

type sendRecord struct {
	start time.Time
	user  int
	chat  string
	body  string
}

type sim struct {
	app     *app.App
	clients []*client.Client
	roster  *roster
	net     netStats
	start   time.Time

	initial sync.Map // chat id → []int 初始成員
	sent    sync.Map // client msg id → sendRecord
	acked   sync.Map // client msg id → seq

	faultsMu sync.Mutex
	faults   []time.Time

	offlineAt, offlineUntil []atomic.Int64 // unix nano

	rejected, offlineUsers, churnOK, churnRejected atomic.Int64
	viaSync                                        atomic.Int64
	steady, duringFault                            metrics.Latency

	orderMu sync.Mutex
	order   map[string][]int64 // user|chat → 依送出順序取得的 seq
}

func run(seed uint64, logger *slog.Logger) error {
	dir, err := os.MkdirTemp("", "realtime-chat-sim-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "chat.db")

	opts := app.DefaultOptions(dbPath)
	opts.Logger = logger
	a, err := app.New(opts)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := a.Start(ctx); err != nil {
		return err
	}

	s := &sim{app: a, roster: newRoster(), order: make(map[string][]int64),
		offlineAt: make([]atomic.Int64, userCount), offlineUntil: make([]atomic.Int64, userCount)}
	for i := range userCount {
		c := client.New(userID(i), func(ctx context.Context) (client.Conn, error) {
			sess, err := a.Connect(ctx, userID(i))
			if err != nil {
				return nil, err
			}
			return newLossyConn(sess, &s.net), nil
		}, client.DefaultConfig())
		c.OnMessage = s.onMessage(i)
		s.clients = append(s.clients, c)
		go c.Run(ctx)
	}

	setupStart := time.Now()
	plans := planChats(rand.New(rand.NewPCG(seed, 1)))
	if err := s.createChats(ctx, plans); err != nil {
		return err
	}
	fmt.Printf("workload  %d users, %s, %d gateways, %d pub/sub servers, %d shards\n",
		userCount, describeChats(plans), opts.Gateways, opts.PubSubServers, opts.Shards)
	fmt.Printf("          chats created and every member subscribed in %s\n\n", time.Since(setupStart).Round(time.Millisecond))

	s.traffic(ctx, seed)

	if err := s.converge(ctx); err != nil {
		fmt.Println("  warning:", err)
	}
	s.report(ctx)
	if err := verify(ctx, s); err != nil {
		return err
	}

	cancel()
	time.Sleep(100 * time.Millisecond)
	a.Close()
	return verifyDurable(dbPath, s)
}

// createChats 由每個聊天室的建立者透過自己的連線建立聊天室，再等所有成員都收到通知。
func (s *sim) createChats(ctx context.Context, plans []chatPlan) error {
	work := make(chan chatPlan)
	var wg sync.WaitGroup
	var failed atomic.Int64
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range work {
				var others []string
				for _, u := range p.members[1:] {
					others = append(others, userID(u))
				}
				cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				id, err := s.clients[p.creator].CreateChat(cctx, p.name, others)
				cancel()
				if err != nil {
					failed.Add(1)
					continue
				}
				s.roster.set(id, p.members)
				s.initial.Store(id, p.members)
			}
		}()
	}
	for _, p := range plans {
		work <- p
	}
	close(work)
	wg.Wait()
	if failed.Load() > 0 {
		return fmt.Errorf("%d chats could not be created", failed.Load())
	}
	return waitFor(30*time.Second, func() bool {
		for u, c := range s.clients {
			known := make(map[string]bool)
			for _, id := range c.Chats() {
				known[id] = true
			}
			for _, id := range s.roster.chatsOf(u) {
				if !known[id] {
					return false
				}
			}
		}
		return true
	})
}

func (s *sim) markFault() {
	s.faultsMu.Lock()
	s.faults = append(s.faults, time.Now())
	s.faultsMu.Unlock()
}

func (s *sim) inFaultWindow(t time.Time) bool {
	s.faultsMu.Lock()
	defer s.faultsMu.Unlock()
	for _, f := range s.faults {
		if !t.Before(f.Add(-time.Second)) && t.Before(f.Add(faultWindow)) {
			return true
		}
	}
	return false
}

func (s *sim) onMessage(u int) func(m protocol.Message, live bool) {
	me := userID(u)
	return func(m protocol.Message, live bool) {
		if m.Kind != string(domain.KindText) || m.SenderID == me {
			return
		}
		if !live {
			s.viaSync.Add(1)
			return
		}
		v, ok := s.sent.Load(m.ClientMsgID)
		if !ok {
			return
		}
		rec := v.(sendRecord)
		if s.offlineAt[rec.user].Load() >= rec.start.UnixNano() {
			return // 發送者送到一半就斷線，延遲裡包含他離線的時間
		}
		if s.inFaultWindow(rec.start) {
			s.duringFault.Observe(time.Since(rec.start))
		} else {
			s.steady.Observe(time.Since(rec.start))
		}
	}
}

// traffic 跑 20 秒的聊天，並依時間表注入故障。
func (s *sim) traffic(ctx context.Context, seed uint64) {
	s.start = time.Now()
	end := s.start.Add(trafficFor)
	var wg sync.WaitGroup
	for u := range userCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.user(ctx, u, end, rand.New(rand.NewPCG(seed, uint64(u)+100)))
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.churn(ctx, end, rand.New(rand.NewPCG(seed, 2)))
	}()

	servers := s.app.Servers()
	schedule := []struct {
		at   time.Duration
		what string
		do   func()
	}{
		{5 * time.Second, "pubsub-1 killed", func() { servers[1].Kill() }},
		{9 * time.Second, "pubsub-2 paused 1.5s (lease is 500ms), wakes up as a zombie owner", func() { servers[2].Pause(1500 * time.Millisecond) }},
		{13 * time.Second, "gateway-0 killed, a replacement joins the load balancer", func() {
			s.app.KillGateway(s.app.Gateways()[0])
			s.app.AddGateway()
		}},
	}
	for _, f := range schedule {
		time.Sleep(time.Until(s.start.Add(f.at)))
		s.markFault()
		f.do()
		fmt.Printf("fault     %4.1fs  %s\n", time.Since(s.start).Seconds(), f.what)
	}
	wg.Wait()
	fmt.Printf("traffic   stopped at %.1fs; every sender's queue drained\n\n", time.Since(s.start).Seconds())
}

// user 依指數分布的間隔發言，一次只送一則：前一則確定送達（必要時重送）才送下一則。
func (s *sim) user(ctx context.Context, u int, end time.Time, rng *rand.Rand) {
	c := s.clients[u]
	if rng.Float64() < offlineRate {
		at := time.Duration(rng.Int64N(int64(trafficFor - 4*time.Second)))
		d := time.Second + time.Duration(rng.Int64N(int64(3*time.Second)))
		s.offlineUsers.Add(1)
		go func() {
			time.Sleep(time.Until(s.start.Add(at)))
			s.offlineAt[u].Store(time.Now().UnixNano())
			s.offlineUntil[u].Store(time.Now().Add(d).UnixNano())
			c.GoOffline(d)
		}()
	}
	for n := 0; ; n++ {
		wait := time.Duration(rng.ExpFloat64() * float64(sendInterval))
		if time.Now().Add(wait).After(end) {
			return
		}
		time.Sleep(wait)
		if time.Now().UnixNano() < s.offlineUntil[u].Load() {
			continue // 沒有訊號時不發言，延遲統計才不會把離線時間算進去
		}
		chats := s.roster.chatsOf(u)
		if len(chats) == 0 {
			continue
		}
		chat := chats[rng.IntN(len(chats))]
		id := c.NewMessageID()
		body := fmt.Sprintf("%s #%d", userID(u), n)
		s.sent.Store(id, sendRecord{start: time.Now(), user: u, chat: chat, body: body})
		seq, err := c.Send(ctx, chat, id, body)
		switch {
		case err == nil:
			s.acked.Store(id, seq)
			s.orderMu.Lock()
			s.order[userID(u)+"|"+chat] = append(s.order[userID(u)+"|"+chat], seq)
			s.orderMu.Unlock()
		case errors.Is(err, domain.ErrNotMember):
			s.rejected.Add(1) // 送出前剛好被移出聊天室
			s.sent.Delete(id)
		default:
			s.sent.Delete(id)
		}
	}
}

// churn 持續在群組裡加人、移除人，由一位目前的成員發起。
func (s *sim) churn(ctx context.Context, end time.Time, rng *rand.Rand) {
	for time.Now().Before(end) {
		time.Sleep(churnEvery)
		var groups []string
		s.initial.Range(func(k, v any) bool {
			if len(v.([]int)) > 2 {
				groups = append(groups, k.(string))
			}
			return true
		})
		chat := groups[rng.IntN(len(groups))]
		members := s.roster.membersOf(chat)
		if len(members) < 2 {
			continue
		}
		actor := members[rng.IntN(len(members))]
		target, op := members[rng.IntN(len(members))], domain.OpRemove
		if rng.Float64() < 0.5 || target == actor {
			target, op = rng.IntN(userCount), domain.OpAdd
		}
		_, err := s.clients[actor].Modify(ctx, chat, userID(target), op)
		switch {
		case err == nil && op == domain.OpAdd:
			s.roster.add(chat, target)
			s.churnOK.Add(1)
		case err == nil:
			s.roster.remove(chat, target)
			s.churnOK.Add(1)
		default:
			s.churnRejected.Add(1)
		}
	}
}
