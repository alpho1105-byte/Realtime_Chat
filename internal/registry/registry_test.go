package registry

import (
	"testing"
	"time"
)

func owners(t Table) map[string]int {
	out := make(map[string]int)
	for _, a := range t.Shards {
		out[a.Server]++
	}
	return out
}

func TestRegisterSpreadsShardsAndBumpsEpochs(t *testing.T) {
	r := New(12, time.Second)
	r.Register("a")
	r.Register("b")
	r.Register("c")
	tbl := r.Table()
	if got := owners(tbl); got["a"] != 4 || got["b"] != 4 || got["c"] != 4 {
		t.Fatalf("owners = %v", got)
	}
	for i, a := range tbl.Shards {
		// 每個 shard 至少換過一次 owner（從沒有 owner 到 a），被搬給 b、c 的又多加一。
		if a.Epoch < 1 || (a.Server != "a" && a.Epoch < 2) {
			t.Fatalf("shard %d = %+v", i, a)
		}
	}
}

func TestExpiredLeaseHandsShardsToSurvivors(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	r := New(8, time.Second)
	r.now = func() time.Time { return now }
	r.Register("a")
	r.Register("b")
	before := r.Table()
	watch := r.Watch()
	<-watch

	now = now.Add(600 * time.Millisecond)
	r.Heartbeat("a")
	now = now.Add(600 * time.Millisecond) // b 已經 1.2 秒沒有 heartbeat
	r.mu.Lock()
	r.expire()
	r.reconcile()
	r.mu.Unlock()

	after := <-watch
	if got := owners(after); got["a"] != 8 {
		t.Fatalf("owners = %v", got)
	}
	for i, a := range before.Shards {
		if a.Server == "b" && after.Shards[i].Epoch != a.Epoch+1 {
			t.Fatalf("shard %d epoch %d -> %d", i, a.Epoch, after.Shards[i].Epoch)
		}
	}
	if r.Heartbeat("b") {
		t.Fatal("an expired server must re-register instead of silently resuming")
	}
}
