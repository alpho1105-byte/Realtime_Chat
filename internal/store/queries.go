package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"realtime-chat/internal/domain"
)

// CreateChat 在同一個交易裡建立 chat 與所有成員的 membership。
// 建立時還沒有任何訊息，成員期間從 seq 1 開始。
func (s *Store) CreateChat(ctx context.Context, c domain.Chat, members []string) error {
	return s.Write(ctx, func(t *Tx) error {
		if _, err := t.tx.ExecContext(ctx, `INSERT INTO chats (chat_id, name, created_by, created_at) VALUES (?, ?, ?, ?)`,
			c.ID, c.Name, c.CreatedBy, c.CreatedAt.UnixMilli()); err != nil {
			return err
		}
		for _, u := range members {
			if err := t.Join(c.ID, u, 1); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) Chat(ctx context.Context, chatID string) (domain.Chat, error) {
	var c domain.Chat
	var created int64
	err := s.r.QueryRowContext(ctx, `SELECT chat_id, name, created_by, created_at FROM chats WHERE chat_id = ?`, chatID).
		Scan(&c.ID, &c.Name, &c.CreatedBy, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return c, domain.ErrNotFound
	}
	c.CreatedAt = time.UnixMilli(created)
	return c, err
}

// ChatsOf 回傳使用者參與過的所有 chat。已經被移除的也要列出：離線期間被移除的人，
// 回來時仍然要補到移除前的訊息。
func (s *Store) ChatsOf(ctx context.Context, userID string) ([]domain.ChatState, error) {
	rows, err := s.r.QueryContext(ctx, `
		SELECT m.chat_id, c.name, MAX(m.left_seq = 0), COALESCE(MAX(r.last_ack), 0)
		FROM memberships m
		JOIN chats c ON c.chat_id = m.chat_id
		LEFT JOIN read_state r ON r.chat_id = m.chat_id AND r.user_id = m.user_id
		WHERE m.user_id = ?
		GROUP BY m.chat_id, c.name
		ORDER BY m.chat_id`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ChatState
	for rows.Next() {
		var st domain.ChatState
		if err := rows.Scan(&st.ChatID, &st.Name, &st.Member, &st.LastAck); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// IsMember 回傳使用者目前是否為成員。
func (s *Store) IsMember(ctx context.Context, chatID, userID string) (bool, error) {
	var n int
	err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM memberships WHERE chat_id = ? AND user_id = ? AND left_seq = 0`,
		chatID, userID).Scan(&n)
	return n > 0, err
}

// Sync 回傳 after 之後、使用者在成員期間內看得到的訊息。covered 是這次回應涵蓋到的位置：
// (after, covered] 之間看不到的 seq 就是不屬於他的，client 不必再等。
func (s *Store) Sync(ctx context.Context, chatID, userID string, after int64, limit int) (msgs []domain.Message, covered int64, hasMore bool, err error) {
	tx, err := s.r.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, false, err
	}
	defer tx.Rollback()

	var periods int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM memberships WHERE chat_id = ? AND user_id = ?`, chatID, userID).Scan(&periods); err != nil {
		return nil, 0, false, err
	}
	if periods == 0 {
		return nil, 0, false, domain.ErrNotMember
	}
	var maxSeq sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(seq) FROM messages WHERE chat_id = ?`, chatID).Scan(&maxSeq); err != nil {
		return nil, 0, false, err
	}
	covered = max(after, maxSeq.Int64)

	rows, err := tx.QueryContext(ctx, `
		SELECT m.chat_id, m.seq, m.sender_id, m.client_msg_id, m.kind, m.body, m.target, m.created_at
		FROM messages m
		JOIN memberships p ON p.chat_id = m.chat_id AND p.user_id = ?
			AND m.seq >= p.joined_seq AND (p.left_seq = 0 OR m.seq <= p.left_seq)
		WHERE m.chat_id = ? AND m.seq > ? AND m.seq <= ?
		ORDER BY m.seq
		LIMIT ?`, userID, chatID, after, covered, limit+1)
	if err != nil {
		return nil, 0, false, err
	}
	msgs, err = scanMessages(rows)
	if err != nil {
		return nil, 0, false, err
	}
	if len(msgs) > limit {
		msgs = msgs[:limit]
		covered, hasMore = msgs[limit-1].Seq, true
	}
	return msgs, covered, hasMore, nil
}

// Messages 回傳 chat 的完整日誌，給驗證與除錯使用。
func (s *Store) Messages(ctx context.Context, chatID string) ([]domain.Message, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT chat_id, seq, sender_id, client_msg_id, kind, body, target, created_at
		FROM messages WHERE chat_id = ? ORDER BY seq`, chatID)
	if err != nil {
		return nil, err
	}
	return scanMessages(rows)
}

func scanMessages(rows *sql.Rows) ([]domain.Message, error) {
	defer rows.Close()
	var out []domain.Message
	for rows.Next() {
		var m domain.Message
		var kind string
		var created int64
		if err := rows.Scan(&m.ChatID, &m.Seq, &m.SenderID, &m.ClientMsgID, &kind, &m.Body, &m.Target, &created); err != nil {
			return nil, err
		}
		m.Kind, m.CreatedAt = domain.Kind(kind), time.UnixMilli(created)
		out = append(out, m)
	}
	return out, rows.Err()
}

type Ack struct {
	ChatID string
	UserID string
	Seq    int64
}

// SaveAcks 批次更新已讀位置。只會往前：晚到的舊 ack 不會把位置拉回去。
func (s *Store) SaveAcks(ctx context.Context, acks []Ack) error {
	return s.Write(ctx, func(t *Tx) error {
		for _, a := range acks {
			if _, err := t.tx.ExecContext(ctx, `INSERT INTO read_state (chat_id, user_id, last_ack) VALUES (?, ?, ?)
				ON CONFLICT (chat_id, user_id) DO UPDATE SET last_ack = MAX(last_ack, excluded.last_ack)`,
				a.ChatID, a.UserID, a.Seq); err != nil {
				return err
			}
		}
		return nil
	})
}
