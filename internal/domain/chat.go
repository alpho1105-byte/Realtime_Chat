package domain

import (
	"errors"
	"time"
)

// MaxParticipants 是一個群組聊天的人數上限。
const MaxParticipants = 100

var (
	ErrNotFound    = errors.New("chat not found")
	ErrNotMember   = errors.New("not a member of this chat")
	ErrChatFull    = errors.New("chat is full")
	ErrAlreadyIn   = errors.New("user is already a member")
	ErrInvalid     = errors.New("invalid request")
	ErrNotOwner    = errors.New("server does not own this shard")
	ErrUnavailable = errors.New("server unavailable")
	ErrFenced      = errors.New("shard ownership moved to a newer epoch")
)

type Kind string

const (
	KindText          Kind = "text"
	KindMemberAdded   Kind = "member_added"
	KindMemberRemoved Kind = "member_removed"
)

// Message 是聊天室日誌裡的一筆紀錄。成員異動也是日誌的一部分，
// 所以「誰看得到哪些訊息」由同一份順序決定，每個人看到的都一樣。
type Message struct {
	ChatID string
	// Seq 由 chat 的 owner 指派，在同一個 chat 內從 1 開始連續遞增。
	Seq         int64
	SenderID    string
	ClientMsgID string
	Kind        Kind
	Body        string
	// Target 是成員異動的對象。
	Target    string
	CreatedAt time.Time
}

type Chat struct {
	ID        string
	Name      string
	CreatedBy string
	CreatedAt time.Time
}

// ChatState 是某位使用者在某個 chat 的狀態，連線時用來決定要補哪些訊息。
type ChatState struct {
	ChatID  string
	Name    string
	Member  bool
	LastAck int64
}

// Topic 是 pub/sub 的主題：chat 主題傳遞訊息，user 主題通知「你被加進了某個 chat」。
func ChatTopic(chatID string) string { return "chat:" + chatID }
func UserTopic(userID string) string { return "user:" + userID }

type Op string

const (
	OpAdd    Op = "ADD"
	OpRemove Op = "REMOVE"
)
