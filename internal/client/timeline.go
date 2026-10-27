package client

import (
	"slices"

	"realtime-chat/internal/protocol"
)

// Timeline 是 client 端一個 chat 的訊息列表，依 seq 排序、不重複、不跳號。
//
// 推播可能重複（重新連線時補到的與即時推播重疊）、可能缺號（換 owner 時遺失）、
// 也可能晚到。比預期大的 seq 先暫存起來並要求同步；同步的結果說明了 (after, covered]
// 之間哪些訊息屬於這個使用者，缺的號碼若不在其中，就是他不在聊天室期間的訊息。
type Timeline struct {
	log     []protocol.Message
	next    int64
	pending map[int64]protocol.Message
	syncing bool
}

func newTimeline() *Timeline {
	return &Timeline{next: 1, pending: make(map[int64]protocol.Message)}
}

// Push 加入一則推播，回傳新接上的訊息，以及是否需要同步。
func (t *Timeline) Push(m protocol.Message) (added []protocol.Message, needSync bool) {
	switch {
	case m.Seq < t.next:
		return nil, false
	case m.Seq > t.next:
		t.pending[m.Seq] = m
		return nil, !t.syncing
	}
	t.append(m)
	added = append([]protocol.Message{m}, t.drain()...)
	return added, false
}

// ApplySync 套用同步結果：msgs 是 (after, covered] 之間這個使用者看得到的所有訊息。
func (t *Timeline) ApplySync(msgs []protocol.Message, covered int64) (added []protocol.Message) {
	for _, m := range msgs {
		if m.Seq >= t.next {
			t.append(m)
			added = append(added, m)
		}
	}
	t.next = max(t.next, covered+1)
	for seq := range t.pending {
		if seq < t.next {
			delete(t.pending, seq)
		}
	}
	return append(added, t.drain()...)
}

// HasGap 回傳是否還有暫存中、等著前面訊息補齊的推播。
func (t *Timeline) HasGap() bool {
	return len(t.pending) > 0
}

// Last 回傳已依序收到的最後一個位置，用於 ack 與同步的起點。
func (t *Timeline) Last() int64 {
	return t.next - 1
}

func (t *Timeline) Messages() []protocol.Message {
	return slices.Clone(t.log)
}

func (t *Timeline) append(m protocol.Message) {
	t.log = append(t.log, m)
	t.next = m.Seq + 1
}

func (t *Timeline) drain() []protocol.Message {
	var added []protocol.Message
	for {
		m, ok := t.pending[t.next]
		if !ok {
			return added
		}
		delete(t.pending, t.next)
		t.append(m)
		added = append(added, m)
	}
}
