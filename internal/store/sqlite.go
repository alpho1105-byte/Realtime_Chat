package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS chats (
	chat_id    TEXT PRIMARY KEY,
	name       TEXT NOT NULL,
	created_by TEXT NOT NULL,
	created_at INTEGER NOT NULL
);

-- 一段成員期間一列。被移除後又被加回來是新的一列，舊的期間仍然決定他看得到哪些舊訊息。
-- 主鍵以 chat_id 開頭：同一個 chat 的成員放在一起，對應 DynamoDB 的 partition key / sort key。
CREATE TABLE IF NOT EXISTS memberships (
	chat_id    TEXT NOT NULL,
	user_id    TEXT NOT NULL,
	joined_seq INTEGER NOT NULL,
	left_seq   INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (chat_id, user_id, joined_seq)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS memberships_by_user ON memberships (user_id, chat_id);
CREATE UNIQUE INDEX IF NOT EXISTS memberships_current ON memberships (chat_id, user_id) WHERE left_seq = 0;

CREATE TABLE IF NOT EXISTS read_state (
	chat_id  TEXT NOT NULL,
	user_id  TEXT NOT NULL,
	last_ack INTEGER NOT NULL,
	PRIMARY KEY (chat_id, user_id)
) WITHOUT ROWID;

-- (chat_id, seq) 是主鍵：同一個位置只能寫入一則訊息。
-- (chat_id, sender_id, client_msg_id) 唯一：client 重送同一則訊息不會變成兩則。
CREATE TABLE IF NOT EXISTS messages (
	chat_id       TEXT NOT NULL,
	seq           INTEGER NOT NULL,
	sender_id     TEXT NOT NULL,
	client_msg_id TEXT NOT NULL,
	kind          TEXT NOT NULL,
	body          TEXT NOT NULL,
	target        TEXT NOT NULL,
	created_at    INTEGER NOT NULL,
	PRIMARY KEY (chat_id, seq)
) WITHOUT ROWID;
CREATE UNIQUE INDEX IF NOT EXISTS messages_client_id ON messages (chat_id, sender_id, client_msg_id);

-- 每個 shard 目前最新的 ownership epoch，用來擋下已經失去 ownership 的舊 owner。
CREATE TABLE IF NOT EXISTS shard_epochs (
	shard INTEGER PRIMARY KEY,
	epoch INTEGER NOT NULL
);
`

// Store 是訊息、聊天室與成員的持久化儲存。
//
// 寫入走單一連線（SQLite 同時只允許一個 writer），讀取另開連線池：
// WAL 模式下讀不會擋寫，補訊息的大量查詢不會拖慢訊息寫入。
type Store struct {
	w *sql.DB
	r *sql.DB
}

// Open 開啟資料庫。synchronous=FULL：commit 回傳前 WAL 已經 fsync 到磁碟，
// 訊息回覆 SUCCESS 之後就算整台機器斷電也不會遺失。
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	base := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	w, err := open(base+"&_pragma=synchronous(FULL)", 1)
	if err != nil {
		return nil, err
	}
	if _, err := w.Exec(schema); err != nil {
		w.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	r, err := open(base+"&_pragma=query_only(1)", 8)
	if err != nil {
		w.Close()
		return nil, err
	}
	return &Store{w: w, r: r}, nil
}

func open(dsn string, conns int) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	return db, nil
}

func (s *Store) Close() error {
	s.r.Close()
	return s.w.Close()
}

// Write 在一個交易裡執行 fn；fn 回傳錯誤時整個交易 rollback。
func (s *Store) Write(ctx context.Context, fn func(*Tx) error) error {
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(&Tx{ctx: ctx, tx: tx}); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
