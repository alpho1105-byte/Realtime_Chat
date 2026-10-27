package client

import (
	"slices"
	"testing"

	"realtime-chat/internal/protocol"
)

func msg(seq int64) protocol.Message {
	return protocol.Message{ChatID: "c1", Seq: seq, Kind: "text"}
}

func seqs(msgs []protocol.Message) []int64 {
	out := make([]int64, len(msgs))
	for i, m := range msgs {
		out[i] = m.Seq
	}
	return out
}

func TestPushDedupesAndBuffersOutOfOrder(t *testing.T) {
	tl := newTimeline()
	if added, sync := tl.Push(msg(1)); len(added) != 1 || sync {
		t.Fatalf("push 1: added=%v sync=%v", seqs(added), sync)
	}
	if added, _ := tl.Push(msg(1)); len(added) != 0 {
		t.Fatal("duplicate push was added again")
	}
	// 3 先到：先暫存並要求同步；2 補上之後兩則一起接上。
	if added, sync := tl.Push(msg(3)); len(added) != 0 || !sync {
		t.Fatalf("push 3: added=%v sync=%v", seqs(added), sync)
	}
	if added, _ := tl.Push(msg(2)); !slices.Equal(seqs(added), []int64{2, 3}) {
		t.Fatalf("push 2: added=%v", seqs(added))
	}
	if tl.HasGap() || tl.Last() != 3 {
		t.Fatalf("gap=%v last=%d", tl.HasGap(), tl.Last())
	}
}

func TestSyncSkipsSeqsOutsideMembership(t *testing.T) {
	tl := newTimeline()
	tl.Push(msg(1))
	tl.Push(msg(2))
	tl.Push(msg(9)) // 推播亂入，前面缺號
	// 使用者在 3..6 之間不在聊天室：同步只回 7、8，並說明涵蓋到 8。
	added := tl.ApplySync([]protocol.Message{msg(7), msg(8)}, 8)
	if !slices.Equal(seqs(added), []int64{7, 8, 9}) {
		t.Fatalf("added = %v", seqs(added))
	}
	if !slices.Equal(seqs(tl.Messages()), []int64{1, 2, 7, 8, 9}) || tl.HasGap() {
		t.Fatalf("timeline = %v, gap=%v", seqs(tl.Messages()), tl.HasGap())
	}
}

func TestSyncDropsPendingPushesItDoesNotConfirm(t *testing.T) {
	tl := newTimeline()
	tl.Push(msg(1))
	tl.Push(msg(4)) // 不屬於這位使用者的推播（例如他已經被移除）
	added := tl.ApplySync(nil, 5)
	if len(added) != 0 || tl.HasGap() || tl.Last() != 5 {
		t.Fatalf("added=%v gap=%v last=%d", seqs(added), tl.HasGap(), tl.Last())
	}
	if !slices.Equal(seqs(tl.Messages()), []int64{1}) {
		t.Fatalf("timeline = %v", seqs(tl.Messages()))
	}
}
