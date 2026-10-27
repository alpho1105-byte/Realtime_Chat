package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"realtime-chat/internal/domain"
)

// BenchmarkWrite 比較一次交易寫一則與寫一批：synchronous=FULL 下每次 commit 都要 fsync，
// 批次寫入讓多則訊息分攤同一次 fsync。
func BenchmarkWrite(b *testing.B) {
	for _, size := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			st, err := Open(b.TempDir() + "/chat.db")
			if err != nil {
				b.Fatal(err)
			}
			defer st.Close()
			ctx := context.Background()
			var seq int64
			b.ResetTimer()
			for range b.N {
				st.Write(ctx, func(tx *Tx) error {
					for range size {
						seq++
						if err := tx.Insert(domain.Message{ChatID: "c1", Seq: seq, SenderID: "a", ClientMsgID: fmt.Sprint(seq),
							Kind: domain.KindText, Body: "hello", CreatedAt: time.Now()}); err != nil {
							return err
						}
					}
					return nil
				})
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N*size), "µs/msg")
		})
	}
}
