// Package registry 模擬 ZooKeeper / etcd 這類強一致的 service discovery：
// pub/sub server 以 heartbeat 維持租約，registry 維護 shard → owner 的對應表，
// 租約過期就把該 server 的 shard 交給其他存活的 server，並把 epoch 加一。
package registry

import (
	"context"
	"hash/fnv"
	"slices"
	"sync"
	"time"
)

type Assignment struct {
	Server string
	// Epoch 每換一次 owner 就加一。寫入訊息時帶著它，儲存層據此擋下舊 owner。
	Epoch int64
}

type Table struct {
	Version int64
	Shards  []Assignment
}

func (t Table) Owner(shard int) Assignment {
	return t.Shards[shard]
}

// ShardOf 把 topic 對應到 shard：shard_id = hash(topic) % S。
func ShardOf(topic string, shards int) int {
	h := fnv.New32a()
	h.Write([]byte(topic))
	return int(h.Sum32() % uint32(shards))
}

type Registry struct {
	mu       sync.Mutex
	ttl      time.Duration
	now      func() time.Time
	members  map[string]time.Time
	table    Table
	watchers []chan Table
}

func New(shards int, ttl time.Duration) *Registry {
	return &Registry{ttl: ttl, now: time.Now, members: make(map[string]time.Time),
		table: Table{Shards: make([]Assignment, shards)}}
}

func (r *Registry) Shards() int {
	return len(r.table.Shards)
}

// Register 加入新的 server：先接手沒有 owner 的 shard，再從負責最多 shard 的 server 搬一些過來，
// 讓每台 server 的 shard 數量差不多。被搬走的 shard 一樣 epoch 加一，舊 owner 之後的寫入會被擋下。
func (r *Registry) Register(server string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.members[server] = r.now()
	r.reconcile()

	target := len(r.table.Shards) / len(r.members)
	load := r.load()
	moved := false
	for load[server] < target {
		busiest := server
		for s, n := range load {
			if n > load[busiest] || (n == load[busiest] && s < busiest) {
				busiest = s
			}
		}
		if load[busiest] <= target {
			break
		}
		for i, a := range r.table.Shards {
			if a.Server == busiest {
				r.table.Shards[i] = Assignment{Server: server, Epoch: a.Epoch + 1}
				break
			}
		}
		load[busiest]--
		load[server]++
		moved = true
	}
	if moved {
		r.publish()
	}
}

// Heartbeat 延長租約。租約已經過期的 server 必須重新 Register，原本的 shard 不會還給它。
func (r *Registry) Heartbeat(server string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[server]; !ok {
		return false
	}
	r.members[server] = r.now()
	return true
}

func (r *Registry) Table() Table {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.copyTable()
}

// Watch 回傳對應表的更新通知。channel 只保留最新的一份，消費者慢也不會讓 registry 卡住。
func (r *Registry) Watch() <-chan Table {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan Table, 1)
	ch <- r.copyTable()
	r.watchers = append(r.watchers, ch)
	return ch
}

// Run 定期檢查租約，直到 ctx 結束。
func (r *Registry) Run(ctx context.Context) {
	ticker := time.NewTicker(r.ttl / 5)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.mu.Lock()
			r.expire()
			r.reconcile()
			r.mu.Unlock()
		}
	}
}

func (r *Registry) expire() {
	cutoff := r.now().Add(-r.ttl)
	for s, last := range r.members {
		if last.Before(cutoff) {
			delete(r.members, s)
		}
	}
}

func (r *Registry) load() map[string]int {
	load := make(map[string]int, len(r.members))
	for s := range r.members {
		load[s] = 0
	}
	for _, a := range r.table.Shards {
		if _, alive := r.members[a.Server]; alive {
			load[a.Server]++
		}
	}
	return load
}

// reconcile 把沒有存活 owner 的 shard 交給目前負責 shard 最少的 server。
func (r *Registry) reconcile() {
	if len(r.members) == 0 {
		return
	}
	load := r.load()
	var orphans []int
	for i, a := range r.table.Shards {
		if _, alive := r.members[a.Server]; !alive {
			orphans = append(orphans, i)
		}
	}
	if len(orphans) == 0 {
		return
	}
	servers := make([]string, 0, len(load))
	for s := range load {
		servers = append(servers, s)
	}
	slices.Sort(servers)
	for _, shard := range orphans {
		least := servers[0]
		for _, s := range servers[1:] {
			if load[s] < load[least] {
				least = s
			}
		}
		load[least]++
		r.table.Shards[shard] = Assignment{Server: least, Epoch: r.table.Shards[shard].Epoch + 1}
	}
	r.publish()
}

func (r *Registry) publish() {
	r.table.Version++
	t := r.copyTable()
	for _, ch := range r.watchers {
		select {
		case <-ch:
		default:
		}
		ch <- t
	}
}

func (r *Registry) copyTable() Table {
	return Table{Version: r.table.Version, Shards: slices.Clone(r.table.Shards)}
}
