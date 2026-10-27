package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"realtime-chat/internal/domain"
)

// Tx 是 pub/sub owner 的一次批次寫入。一批訊息共用一個交易，也就共用一次 fsync。
type Tx struct {
	ctx context.Context
	tx  *sql.Tx
}

// Fence 確認寫入者仍然持有 shard 的最新 epoch，並把 epoch 記錄下來。
// 新 owner 接手時先用新的 epoch 呼叫一次，之後舊 owner（例如 GC 停頓後醒來的 zombie）
// 帶著舊 epoch 寫入都會失敗。
func (t *Tx) Fence(shard int, epoch int64) error {
	var stored int64
	err := t.tx.QueryRowContext(t.ctx, `SELECT epoch FROM shard_epochs WHERE shard = ?`, shard).Scan(&stored)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return err
	case stored > epoch:
		return domain.ErrFenced
	case stored == epoch:
		return nil
	}
	_, err = t.tx.ExecContext(t.ctx, `INSERT INTO shard_epochs (shard, epoch) VALUES (?, ?)
		ON CONFLICT (shard) DO UPDATE SET epoch = excluded.epoch`, shard, epoch)
	return err
}

func (t *Tx) MaxSeq(chatID string) (int64, error) {
	var seq sql.NullInt64
	err := t.tx.QueryRowContext(t.ctx, `SELECT MAX(seq) FROM messages WHERE chat_id = ?`, chatID).Scan(&seq)
	return seq.Int64, err
}

// FindClientMsg 找出同一位發送者先前用同一個 client_msg_id 送出的訊息。
func (t *Tx) FindClientMsg(chatID, senderID, clientMsgID string) (int64, bool, error) {
	var seq int64
	err := t.tx.QueryRowContext(t.ctx, `SELECT seq FROM messages WHERE chat_id = ? AND sender_id = ? AND client_msg_id = ?`,
		chatID, senderID, clientMsgID).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return seq, err == nil, err
}

func (t *Tx) IsMember(chatID, userID string) (bool, error) {
	var n int
	err := t.tx.QueryRowContext(t.ctx, `SELECT COUNT(*) FROM memberships WHERE chat_id = ? AND user_id = ? AND left_seq = 0`,
		chatID, userID).Scan(&n)
	return n > 0, err
}

func (t *Tx) MemberCount(chatID string) (int, error) {
	var n int
	err := t.tx.QueryRowContext(t.ctx, `SELECT COUNT(*) FROM memberships WHERE chat_id = ? AND left_seq = 0`, chatID).Scan(&n)
	return n, err
}

func (t *Tx) Insert(m domain.Message) error {
	_, err := t.tx.ExecContext(t.ctx, `INSERT INTO messages (chat_id, seq, sender_id, client_msg_id, kind, body, target, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ChatID, m.Seq, m.SenderID, m.ClientMsgID, string(m.Kind), m.Body, m.Target, m.CreatedAt.UnixMilli())
	if err != nil {
		return fmt.Errorf("insert message %s/%d: %w", m.ChatID, m.Seq, err)
	}
	return nil
}

// Join 從 seq 開始一段新的成員期間：加入的人看得到「自己被加入」這則訊息之後的內容。
func (t *Tx) Join(chatID, userID string, seq int64) error {
	_, err := t.tx.ExecContext(t.ctx, `INSERT INTO memberships (chat_id, user_id, joined_seq) VALUES (?, ?, ?)`, chatID, userID, seq)
	return err
}

// Leave 結束目前的成員期間：被移除的人仍然看得到「自己被移除」這則訊息。
func (t *Tx) Leave(chatID, userID string, seq int64) error {
	_, err := t.tx.ExecContext(t.ctx, `UPDATE memberships SET left_seq = ? WHERE chat_id = ? AND user_id = ? AND left_seq = 0`,
		seq, chatID, userID)
	return err
}
