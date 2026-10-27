// Package protocol 定義 client 與 gateway 之間 WebSocket 上傳遞的 JSON frame。
// WebSocket 是雙向的長連線，所以不走 REST：請求帶 reqId，回應用同一個 reqId 對應，
// server 也可以隨時主動推送 newMessage 等 frame。
package protocol

import "realtime-chat/internal/domain"

// client → server
const (
	TypeCreateChat = "createChat"
	TypeSend       = "sendMessage"
	TypeModify     = "modifyChatParticipants"
	TypeSync       = "sync"
	TypeAck        = "ack"
)

// server → client
const (
	TypeResponse   = "response"
	TypeNewMessage = "newMessage"
	TypeChatList   = "chatList"
	TypeChatAdded  = "chatAdded"
	// TypeResync 請 client 重新同步某個 chat：gateway 改向新的 owner 訂閱時，中間可能漏掉推播。
	TypeResync = "resync"
)

const (
	StatusSuccess = "SUCCESS"
	StatusFailure = "FAILURE"
)

type ClientFrame struct {
	Type         string   `json:"type"`
	ReqID        string   `json:"reqId,omitempty"`
	ChatID       string   `json:"chatId,omitempty"`
	Participants []string `json:"participants,omitempty"`
	Name         string   `json:"name,omitempty"`
	// ClientMsgID 由 client 產生，重送時不變，server 靠它去重。
	ClientMsgID string `json:"clientMsgId,omitempty"`
	Message     string `json:"message,omitempty"`
	UserID      string `json:"userId,omitempty"`
	Operation   string `json:"operation,omitempty"`
	// Seq：ack 時是已依序收到的最大 seq；sync 時是要從哪個 seq 之後開始補。
	Seq int64 `json:"seq,omitempty"`
}

type ServerFrame struct {
	Type   string `json:"type"`
	ReqID  string `json:"reqId,omitempty"`
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
	ChatID string `json:"chatId,omitempty"`
	// Seq：sendMessage 回應中是訊息的 message id；sync 回應中是這次同步涵蓋到的 seq。
	Seq      int64      `json:"seq,omitempty"`
	HasMore  bool       `json:"hasMore,omitempty"`
	Message  *Message   `json:"message,omitempty"`
	Messages []Message  `json:"messages,omitempty"`
	Chats    []ChatInfo `json:"chats,omitempty"`
}

type Message struct {
	ChatID    string `json:"chatId"`
	Seq       int64  `json:"messageId"`
	SenderID  string `json:"senderId"`
	Kind      string `json:"kind"`
	Body      string `json:"body,omitempty"`
	Target    string `json:"target,omitempty"`
	CreatedAt int64  `json:"createdAt"`
	// ClientMsgID 讓發送者認出自己送出的訊息。
	ClientMsgID string `json:"clientMsgId,omitempty"`
}

type ChatInfo struct {
	ChatID  string `json:"chatId"`
	Name    string `json:"name"`
	Member  bool   `json:"member"`
	LastAck int64  `json:"lastAck"`
}

func FromMessage(m domain.Message) Message {
	return Message{ChatID: m.ChatID, Seq: m.Seq, SenderID: m.SenderID, Kind: string(m.Kind), Body: m.Body,
		Target: m.Target, CreatedAt: m.CreatedAt.UnixMilli(), ClientMsgID: m.ClientMsgID}
}
