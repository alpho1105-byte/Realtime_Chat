package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"realtime-chat/internal/domain"
	"realtime-chat/internal/store"
)

var failures int

func check(desc string, ok bool) {
	mark := "ok  "
	if !ok {
		mark = "FAIL"
		failures++
	}
	fmt.Printf("  [%s] %s\n", mark, desc)
}

func waitFor(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return errors.New("timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func describeChats(plans []chatPlan) string {
	var dm, small, medium, large, biggest int
	for _, p := range plans {
		n := len(p.members)
		biggest = max(biggest, n)
		switch {
		case n == 2:
			dm++
		case n <= 10:
			small++
		case n <= 40:
			medium++
		default:
			large++
		}
	}
	return fmt.Sprintf("%d chats (%d one-to-one, %d of 3-10, %d of 11-40, %d of 41-%d)", len(plans), dm, small, medium, large, biggest)
}

// truth 是以儲存層日誌重播出來的正確答案：每位使用者在每個聊天室應該看到的訊息。
// 成員異動本身就在日誌裡，所以「誰看得到哪一則」只有一種答案。
type truth struct {
	logs    map[string][]domain.Message
	visible map[string]map[string][]int64 // user → chat → seq
	members int64                         // 每則文字訊息送達時的成員數總和
	texts   int64
}

func buildTruth(ctx context.Context, s *sim) (truth, error) {
	t := truth{logs: make(map[string][]domain.Message), visible: make(map[string]map[string][]int64)}
	var err error
	s.initial.Range(func(k, v any) bool {
		chat := k.(string)
		var log []domain.Message
		log, err = s.app.Store.Messages(ctx, chat)
		if err != nil {
			return false
		}
		t.logs[chat] = log
		current := make(map[string]bool)
		for _, u := range v.([]int) {
			current[userID(u)] = true
			t.see(userID(u), chat, 0)
		}
		for _, m := range log {
			if m.Kind == domain.KindMemberAdded {
				current[m.Target] = true
			}
			for u := range current {
				t.see(u, chat, m.Seq)
			}
			if m.Kind == domain.KindText {
				t.texts++
				t.members += int64(len(current))
			}
			if m.Kind == domain.KindMemberRemoved {
				delete(current, m.Target)
			}
		}
		return true
	})
	return t, err
}

// see 記錄 user 看得到 chat 的 seq；seq 0 只是讓沒有訊息的聊天室也出現在名單上。
func (t truth) see(user, chat string, seq int64) {
	if t.visible[user] == nil {
		t.visible[user] = make(map[string][]int64)
	}
	if seq == 0 {
		if _, ok := t.visible[user][chat]; !ok {
			t.visible[user][chat] = []int64{}
		}
		return
	}
	t.visible[user][chat] = append(t.visible[user][chat], seq)
}

// converge 等每位使用者都收到應該收到的訊息（離線的人回來後要補完）。
func (s *sim) converge(ctx context.Context) error {
	start := time.Now()
	err := waitFor(30*time.Second, func() bool {
		t, err := buildTruth(ctx, s)
		if err != nil {
			return false
		}
		for u, c := range s.clients {
			for chat, seqs := range t.visible[userID(u)] {
				got := c.Timeline(chat)
				if len(got) != len(seqs) || (len(seqs) > 0 && got[len(got)-1].Seq != seqs[len(seqs)-1]) {
					return false
				}
			}
		}
		return true
	})
	fmt.Printf("converge  every client caught up %s after traffic stopped\n", time.Since(start).Round(time.Millisecond))
	return err
}

func (s *sim) report(ctx context.Context) {
	var committed, dups, fenced, batches, pushes int64
	for _, srv := range s.app.Servers() {
		st := srv.Stats()
		committed += st.Committed
		dups += st.Duplicates
		fenced += st.Fenced
		batches += st.Batches
		pushes += st.Pushes
	}
	var retries int64
	for _, c := range s.clients {
		retries += c.Retries()
	}
	acked := 0
	s.acked.Range(func(_, _ any) bool { acked++; return true })

	fmt.Printf("ingest    %d messages acknowledged, %d membership changes (%d rejected: already in / not a member), %d sends rejected because the sender had just been removed\n",
		acked, s.churnOK.Load(), s.churnRejected.Load(), s.rejected.Load())
	fmt.Printf("          network lost %d requests and %d responses; clients retried %d times, %d retries matched an already stored message\n",
		s.net.lostRequests.Load(), s.net.lostResponses.Load(), retries, dups)
	fmt.Printf("          %d users went offline for 1-4s and caught up through sync (%d messages)\n", s.offlineUsers.Load(), s.viaSync.Load())
	fmt.Printf("store     %d writes in %d batches (%.1f per fsync); %d requests from the zombie owner rejected by the epoch fence\n",
		committed, batches, float64(committed)/float64(max(1, batches)), fenced)
	st, ft := s.steady.Summary(), s.duringFault.Summary()
	fmt.Printf("latency   send -> online recipient: p50=%s p95=%s p99=%s max=%s (%d deliveries)\n", ms(st.P50), ms(st.P95), ms(st.P99), ms(st.Max), st.Count)
	fmt.Printf("          sent within 1s before / 2s after a fault: p50=%s p99=%s max=%s (%d deliveries)\n", ms(ft.P50), ms(ft.P99), ms(ft.Max), ft.Count)
	if t, err := buildTruth(ctx, s); err == nil {
		fmt.Printf("fan-out   a message has %.1f members on average but goes to %.1f gateways (%d pushes for %d writes)\n\n",
			float64(t.members)/float64(max(1, t.texts)), float64(pushes)/float64(max(1, committed)), pushes, committed)
	}
}

func ms(d time.Duration) string {
	if d >= time.Second {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}

func verify(ctx context.Context, s *sim) error {
	fmt.Println("verify")
	t, err := buildTruth(ctx, s)
	if err != nil {
		return err
	}

	// 1. 每則回覆 SUCCESS 的訊息，剛好存了一次，位置就是回給發送者的 message id。
	byClientID := make(map[string][]domain.Message)
	for _, log := range t.logs {
		for _, m := range log {
			if m.Kind == domain.KindText {
				byClientID[m.ClientMsgID] = append(byClientID[m.ClientMsgID], m)
			}
		}
	}
	acked, wrong := 0, 0
	s.acked.Range(func(k, v any) bool {
		acked++
		rec, _ := s.sent.Load(k)
		ms := byClientID[k.(string)]
		if len(ms) != 1 || ms[0].Seq != v.(int64) || ms[0].Body != rec.(sendRecord).body || ms[0].ChatID != rec.(sendRecord).chat {
			wrong++
		}
		return true
	})
	stored := 0
	for _, ms := range byClientID {
		stored += len(ms)
	}
	check(fmt.Sprintf("every acknowledged message is stored exactly once, at the id its sender was given (%d messages)", acked),
		wrong == 0 && stored == acked)

	// 2. 每個聊天室的 message id 從 1 開始連續，沒有跳號也沒有重複。
	gaps := 0
	for _, log := range t.logs {
		for i, m := range log {
			if m.Seq != int64(i+1) {
				gaps++
				break
			}
		}
	}
	check(fmt.Sprintf("every chat's message ids run 1..N with no gaps (%d chats)", len(t.logs)), gaps == 0)

	// 3. 同一位使用者在同一個聊天室連續送出的訊息，順序和送出順序相同。
	reordered := 0
	s.orderMu.Lock()
	for _, seqs := range s.order {
		if !slices.IsSorted(seqs) {
			reordered++
		}
	}
	s.orderMu.Unlock()
	check("each sender's messages keep the order they were sent in", reordered == 0)

	// 4. 每位使用者最後收到的，剛好是自己成員期間內的所有訊息，依同一個順序，沒有重複。
	timelines, mismatched := 0, 0
	for u, c := range s.clients {
		for chat, seqs := range t.visible[userID(u)] {
			timelines++
			got := c.Timeline(chat)
			ok := len(got) == len(seqs)
			for i := 0; ok && i < len(got); i++ {
				want := t.logs[chat][seqs[i]-1]
				ok = got[i].Seq == seqs[i] && got[i].SenderID == want.SenderID && got[i].Body == want.Body
			}
			if !ok {
				mismatched++
			}
		}
		for _, chat := range c.Chats() {
			if _, ok := t.visible[userID(u)][chat]; !ok && len(c.Timeline(chat)) > 0 {
				mismatched++ // 收到不屬於自己的聊天室的訊息
			}
		}
	}
	check(fmt.Sprintf("every user ended with exactly the messages of their membership periods, in the same order (%d users, %d timelines)",
		len(s.clients), timelines), mismatched == 0)

	var fenced int64
	for _, srv := range s.app.Servers() {
		fenced += srv.Stats().Fenced
	}
	check(fmt.Sprintf("the zombie owner's in-flight batch was rejected by the epoch fence (%d requests), then retried on the new owner", fenced), fenced > 0)

	st := s.steady.Summary()
	check(fmt.Sprintf("online recipients get messages within 500ms outside failover windows (p99 %s)", ms(st.P99)), st.P99 < 500*time.Millisecond)

	if failures > 0 {
		return fmt.Errorf("%d checks failed", failures)
	}
	return nil
}

// verifyDurable 關掉整個系統後重新開啟資料庫：回覆過 SUCCESS 的訊息都必須還在。
func verifyDurable(path string, s *sim) error {
	st, err := store.Open(path)
	if err != nil {
		return err
	}
	defer st.Close()
	byChat := make(map[string]map[string]int64)
	s.initial.Range(func(k, _ any) bool {
		msgs, e := st.Messages(context.Background(), k.(string))
		if e != nil {
			err = e
			return false
		}
		byChat[k.(string)] = make(map[string]int64)
		for _, m := range msgs {
			byChat[k.(string)][m.ClientMsgID] = m.Seq
		}
		return true
	})
	if err != nil {
		return err
	}
	missing, acked := 0, 0
	s.acked.Range(func(k, v any) bool {
		acked++
		rec, _ := s.sent.Load(k)
		if byChat[rec.(sendRecord).chat][k.(string)] != v.(int64) {
			missing++
		}
		return true
	})
	check(fmt.Sprintf("after shutting everything down and reopening the database, all %d acknowledged messages are there", acked), missing == 0)
	if failures > 0 {
		return fmt.Errorf("%d checks failed", failures)
	}
	return nil
}
