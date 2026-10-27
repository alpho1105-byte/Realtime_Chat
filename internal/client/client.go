// Package client 是裝置端的聊天邏輯：維持連線、斷線重連、送出訊息並在失敗時重送、
// 依 seq 排列收到的訊息並在缺號時向 server 同步，以及回報已讀位置。
package client

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"realtime-chat/internal/domain"
	"realtime-chat/internal/protocol"
)

// Conn 是一條到 gateway 的連線（WebSocket 或模擬用的記憶體連線）。
type Conn interface {
	Send(f protocol.ClientFrame) error
	Frames() <-chan protocol.ServerFrame
	Close()
}

type Dialer func(ctx context.Context) (Conn, error)

type Config struct {
	RequestTimeout time.Duration
	RetryBackoff   time.Duration
	ReconnectDelay time.Duration
	AckEvery       time.Duration
}

func DefaultConfig() Config {
	return Config{RequestTimeout: 250 * time.Millisecond, RetryBackoff: 20 * time.Millisecond,
		ReconnectDelay: 50 * time.Millisecond, AckEvery: 200 * time.Millisecond}
}

var errDisconnected = errors.New("disconnected")

// 這些錯誤重送也不會成功，直接回報給使用者。
var permanent = map[string]error{}

func init() {
	for _, err := range []error{domain.ErrNotMember, domain.ErrChatFull, domain.ErrAlreadyIn, domain.ErrInvalid, domain.ErrNotFound} {
		permanent[err.Error()] = err
	}
}

type Client struct {
	user string
	dial Dialer
	cfg  Config
	// OnMessage 在訊息依序接上 timeline 時呼叫；live 表示來自即時推播，而不是同步補回來的。
	OnMessage func(m protocol.Message, live bool)

	mu           sync.Mutex
	conn         Conn
	connected    chan struct{}
	offlineUntil time.Time
	timelines    map[string]*Timeline
	acked        map[string]int64
	waiters      map[string]chan protocol.ServerFrame
	reqSeq       int64
	msgSeq       int64

	retries atomic.Int64
}

func New(user string, dial Dialer, cfg Config) *Client {
	return &Client{user: user, dial: dial, cfg: cfg, connected: make(chan struct{}),
		timelines: make(map[string]*Timeline), acked: make(map[string]int64), waiters: make(map[string]chan protocol.ServerFrame)}
}

func (c *Client) Retries() int64 { return c.retries.Load() }

// Run 維持連線直到 ctx 結束：斷線後自動重連，重連後依 chat 清單補上離線期間的訊息。
func (c *Client) Run(ctx context.Context) {
	for ctx.Err() == nil {
		c.mu.Lock()
		wait := time.Until(c.offlineUntil)
		c.mu.Unlock()
		if wait > 0 {
			sleep(ctx, wait)
			continue
		}
		conn, err := c.dial(ctx)
		if err != nil {
			sleep(ctx, c.cfg.ReconnectDelay)
			continue
		}
		c.serve(ctx, conn)
		sleep(ctx, c.cfg.ReconnectDelay)
	}
}

func (c *Client) serve(ctx context.Context, conn Conn) {
	c.mu.Lock()
	c.conn = conn
	close(c.connected)
	c.mu.Unlock()

	stop := make(chan struct{})
	go c.ackLoop(conn, stop)
	frames := conn.Frames()
loop:
	for {
		select {
		case <-ctx.Done():
			conn.Close()
			break loop
		case f, ok := <-frames:
			if !ok {
				break loop
			}
			c.handle(f)
		}
	}
	close(stop)

	c.mu.Lock()
	c.conn = nil
	c.connected = make(chan struct{})
	for id, w := range c.waiters {
		close(w)
		delete(c.waiters, id)
	}
	for _, t := range c.timelines {
		t.syncing = false
	}
	c.mu.Unlock()
}

// GoOffline 中斷連線並在 d 之後才重新連線，模擬手機沒有訊號。
func (c *Client) GoOffline(d time.Duration) {
	c.mu.Lock()
	c.offlineUntil = time.Now().Add(d)
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

func (c *Client) handle(f protocol.ServerFrame) {
	switch f.Type {
	case protocol.TypeResponse:
		c.mu.Lock()
		w, ok := c.waiters[f.ReqID]
		delete(c.waiters, f.ReqID)
		c.mu.Unlock()
		if ok {
			w <- f
		}
	case protocol.TypeNewMessage:
		c.mu.Lock()
		t := c.timeline(f.ChatID)
		added, needSync := t.Push(*f.Message)
		c.mu.Unlock()
		c.emit(added, true)
		if needSync {
			go c.sync(f.ChatID)
		}
	case protocol.TypeChatList, protocol.TypeChatAdded:
		c.mu.Lock()
		for _, info := range f.Chats {
			if _, known := c.timelines[info.ChatID]; !known {
				c.timeline(info.ChatID).next = info.LastAck + 1
			}
		}
		c.mu.Unlock()
		for _, info := range f.Chats {
			go c.sync(info.ChatID)
		}
	case protocol.TypeResync:
		go c.sync(f.ChatID)
	}
}

func (c *Client) timeline(chatID string) *Timeline {
	t, ok := c.timelines[chatID]
	if !ok {
		t = newTimeline()
		c.timelines[chatID] = t
	}
	return t
}

func (c *Client) emit(msgs []protocol.Message, live bool) {
	if c.OnMessage == nil {
		return
	}
	for _, m := range msgs {
		c.OnMessage(m, live)
	}
}

// sync 從 timeline 已依序收到的位置之後補訊息，直到沒有缺號為止。同一個 chat 同時只跑一個。
func (c *Client) sync(chatID string) {
	c.mu.Lock()
	t := c.timeline(chatID)
	if t.syncing {
		c.mu.Unlock()
		return
	}
	t.syncing = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		t.syncing = false
		c.mu.Unlock()
	}()

	for {
		c.mu.Lock()
		after := t.Last()
		c.mu.Unlock()
		resp, err := c.request(context.Background(), protocol.ClientFrame{Type: protocol.TypeSync, ChatID: chatID, Seq: after})
		if err != nil {
			if errors.Is(err, errDisconnected) {
				return // 重連後的 chat 清單會再觸發同步
			}
			c.retries.Add(1)
			time.Sleep(c.cfg.RetryBackoff)
			continue
		}
		if resp.Status != protocol.StatusSuccess {
			return
		}
		c.mu.Lock()
		added := t.ApplySync(resp.Messages, resp.Seq)
		again := resp.HasMore || t.HasGap()
		c.mu.Unlock()
		c.emit(added, false)
		if !again {
			return
		}
	}
}

func (c *Client) ackLoop(conn Conn, stop <-chan struct{}) {
	ticker := time.NewTicker(c.cfg.AckEvery)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		var acks []protocol.ClientFrame
		c.mu.Lock()
		for id, t := range c.timelines {
			if last := t.Last(); last > c.acked[id] {
				acks = append(acks, protocol.ClientFrame{Type: protocol.TypeAck, ChatID: id, Seq: last})
				c.acked[id] = last
			}
		}
		c.mu.Unlock()
		for _, a := range acks {
			conn.Send(a)
		}
	}
}

// request 送出一個請求並等待回應。逾時不代表沒送到：server 可能已經處理，只是回應遺失。
func (c *Client) request(ctx context.Context, f protocol.ClientFrame) (protocol.ServerFrame, error) {
	c.mu.Lock()
	connected, conn := c.connected, c.conn
	c.mu.Unlock()
	if conn == nil {
		select {
		case <-connected:
		case <-ctx.Done():
			return protocol.ServerFrame{}, ctx.Err()
		case <-time.After(c.cfg.RequestTimeout):
			return protocol.ServerFrame{}, errDisconnected
		}
		c.mu.Lock()
		conn = c.conn
		c.mu.Unlock()
		if conn == nil {
			return protocol.ServerFrame{}, errDisconnected
		}
	}

	c.mu.Lock()
	c.reqSeq++
	f.ReqID = fmt.Sprintf("r%d", c.reqSeq)
	w := make(chan protocol.ServerFrame, 1)
	c.waiters[f.ReqID] = w
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.waiters, f.ReqID)
		c.mu.Unlock()
	}()

	if err := conn.Send(f); err != nil {
		return protocol.ServerFrame{}, errDisconnected
	}
	timer := time.NewTimer(c.cfg.RequestTimeout)
	defer timer.Stop()
	select {
	case resp, ok := <-w:
		if !ok {
			return protocol.ServerFrame{}, errDisconnected
		}
		return resp, nil
	case <-timer.C:
		return protocol.ServerFrame{}, context.DeadlineExceeded
	case <-ctx.Done():
		return protocol.ServerFrame{}, ctx.Err()
	}
}

// do 重送同一個請求直到成功或遇到重送也沒用的錯誤。請求裡的 clientMsgId 不變，
// 所以就算前一次其實已經寫入，server 也只會回傳原本的結果。
func (c *Client) do(ctx context.Context, f protocol.ClientFrame) (protocol.ServerFrame, error) {
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			c.retries.Add(1)
			if err := sleep(ctx, c.cfg.RetryBackoff); err != nil {
				return protocol.ServerFrame{}, err
			}
		}
		resp, err := c.request(ctx, f)
		if err != nil {
			if ctx.Err() != nil {
				return resp, ctx.Err()
			}
			continue
		}
		if resp.Status == protocol.StatusSuccess {
			return resp, nil
		}
		if perr, ok := permanent[resp.Error]; ok {
			return resp, perr
		}
	}
}

// NewMessageID 產生一個新的 clientMsgId。同一則訊息重送時沿用它，server 才認得出是同一則。
func (c *Client) NewMessageID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgSeq++
	return fmt.Sprintf("%s-m%d", c.user, c.msgSeq)
}

// Send 送出一則訊息，回傳 server 指派的 message id。
func (c *Client) Send(ctx context.Context, chatID, clientMsgID, text string) (int64, error) {
	resp, err := c.do(ctx, protocol.ClientFrame{Type: protocol.TypeSend, ChatID: chatID, ClientMsgID: clientMsgID, Message: text})
	return resp.Seq, err
}

func (c *Client) CreateChat(ctx context.Context, name string, participants []string) (string, error) {
	resp, err := c.do(ctx, protocol.ClientFrame{Type: protocol.TypeCreateChat, Name: name, Participants: participants})
	return resp.ChatID, err
}

func (c *Client) Modify(ctx context.Context, chatID, userID string, op domain.Op) (int64, error) {
	resp, err := c.do(ctx, protocol.ClientFrame{Type: protocol.TypeModify, ChatID: chatID, UserID: userID,
		Operation: string(op), ClientMsgID: c.NewMessageID()})
	return resp.Seq, err
}

// Timeline 回傳某個 chat 目前收到的訊息。
func (c *Client) Timeline(chatID string) []protocol.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.timelines[chatID]; ok {
		return t.Messages()
	}
	return nil
}

func (c *Client) Chats() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]string, 0, len(c.timelines))
	for id := range c.timelines {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
