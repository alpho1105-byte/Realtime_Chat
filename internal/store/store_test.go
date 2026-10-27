package store

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"realtime-chat/internal/domain"
)

func openStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "chat.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func text(chat string, seq int64, sender string) domain.Message {
	return domain.Message{ChatID: chat, Seq: seq, SenderID: sender, ClientMsgID: sender + "-" + string(rune('a'+seq)),
		Kind: domain.KindText, Body: "hi", CreatedAt: time.Now()}
}

func TestFenceRejectsOlderEpochs(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	fence := func(epoch int64) error {
		return st.Write(ctx, func(tx *Tx) error { return tx.Fence(0, epoch) })
	}
	if err := fence(2); err != nil {
		t.Fatal(err)
	}
	if err := fence(2); err != nil {
		t.Fatalf("same epoch should keep writing: %v", err)
	}
	if err := fence(1); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("older epoch: err = %v", err)
	}
	if err := fence(3); err != nil {
		t.Fatal(err)
	}
	if err := fence(2); !errors.Is(err, domain.ErrFenced) {
		t.Fatalf("after a newer owner claimed the shard: err = %v", err)
	}
}

func TestSyncReturnsOnlyMessagesFromMembershipPeriods(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	if err := st.CreateChat(ctx, domain.Chat{ID: "c1", Name: "team", CreatedBy: "a", CreatedAt: time.Now()}, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	// b 在 seq 2 被移除、seq 5 被加回來。
	err := st.Write(ctx, func(tx *Tx) error {
		removed := text("c1", 2, "a")
		removed.Kind, removed.Target = domain.KindMemberRemoved, "b"
		added := text("c1", 5, "a")
		added.Kind, added.Target = domain.KindMemberAdded, "b"
		for _, m := range []domain.Message{text("c1", 1, "a"), removed, text("c1", 3, "a"), text("c1", 4, "a"), added, text("c1", 6, "b")} {
			if err := tx.Insert(m); err != nil {
				return err
			}
		}
		if err := tx.Leave("c1", "b", 2); err != nil {
			return err
		}
		return tx.Join("c1", "b", 5)
	})
	if err != nil {
		t.Fatal(err)
	}

	msgs, covered, more, err := st.Sync(ctx, "c1", "b", 0, 100)
	if err != nil || covered != 6 || more {
		t.Fatalf("covered=%d more=%v err=%v", covered, more, err)
	}
	var got []int64
	for _, m := range msgs {
		got = append(got, m.Seq)
	}
	if !slices.Equal(got, []int64{1, 2, 5, 6}) {
		t.Fatalf("b sees %v", got)
	}

	page, covered, more, _ := st.Sync(ctx, "c1", "b", 0, 2)
	if len(page) != 2 || covered != 2 || !more {
		t.Fatalf("first page: %d messages, covered=%d more=%v", len(page), covered, more)
	}
	if _, _, _, err := st.Sync(ctx, "c1", "stranger", 0, 100); !errors.Is(err, domain.ErrNotMember) {
		t.Fatalf("stranger: err = %v", err)
	}
}

func TestClientMessageIDIsUniquePerSender(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	m := text("c1", 1, "a")
	if err := st.Write(ctx, func(tx *Tx) error { return tx.Insert(m) }); err != nil {
		t.Fatal(err)
	}
	m.Seq = 2
	if err := st.Write(ctx, func(tx *Tx) error { return tx.Insert(m) }); err == nil {
		t.Fatal("the same client message id was stored twice")
	}
}

func TestAcksOnlyMoveForward(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	st.CreateChat(ctx, domain.Chat{ID: "c1", Name: "team", CreatedBy: "a", CreatedAt: time.Now()}, []string{"a", "b"})
	st.SaveAcks(ctx, []Ack{{ChatID: "c1", UserID: "b", Seq: 5}})
	st.SaveAcks(ctx, []Ack{{ChatID: "c1", UserID: "b", Seq: 3}})
	states, err := st.ChatsOf(ctx, "b")
	if err != nil || len(states) != 1 || states[0].LastAck != 5 || !states[0].Member {
		t.Fatalf("states = %+v, err = %v", states, err)
	}
}
