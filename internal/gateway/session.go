package gateway

import (
	"context"
	"errors"
	"sync"
	"unicode/utf8"

	"realtime-chat/internal/domain"
	"realtime-chat/internal/protocol"
	"realtime-chat/internal/pubsub"
)

const maxMessageRunes = 4096

// Session 是一條使用者連線。WebSocket handler 與模擬的 client 都透過它收發 frame。
type Session struct {
	gw   *Gateway
	user string

	mu     sync.Mutex
	out    chan protocol.ServerFrame
	closed bool
}

// Frames 是 server 送給 client 的 frame；連線結束時會被關閉。
func (s *Session) Frames() <-chan protocol.ServerFrame {
	return s.out
}

// Send 處理 client 送來的 frame。每個請求在自己的 goroutine 處理，
// 一則等待寫入的訊息不會擋住同一條連線上的 ack 或其他請求。
func (s *Session) Send(f protocol.ClientFrame) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return domain.ErrUnavailable
	}
	if f.Type == protocol.TypeAck {
		if f.ChatID != "" && f.Seq > 0 {
			s.gw.queueAck(f.ChatID, s.user, f.Seq)
		}
		return nil
	}
	go s.handle(f)
	return nil
}

func (s *Session) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.out)
	s.mu.Unlock()
	s.gw.detach(s)
}

// push 不會阻塞。佇列滿了代表 client 跟不上推播：斷開連線，讓它重連後自己補訊息，
// 而不是讓一個慢的 client 拖住整台 gateway。
func (s *Session) push(f protocol.ServerFrame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.out <- f:
	default:
		go s.Close()
	}
}

func (s *Session) handle(f protocol.ClientFrame) {
	ctx, cancel := context.WithTimeout(s.gw.ctx, s.gw.cfg.RequestTimeout)
	defer cancel()
	resp := protocol.ServerFrame{Type: protocol.TypeResponse, ReqID: f.ReqID, ChatID: f.ChatID}
	err := s.dispatch(ctx, f, &resp)
	if err != nil {
		resp.Status, resp.Error = protocol.StatusFailure, err.Error()
	} else {
		resp.Status = protocol.StatusSuccess
	}
	s.push(resp)
}

func (s *Session) dispatch(ctx context.Context, f protocol.ClientFrame, resp *protocol.ServerFrame) error {
	g := s.gw
	switch f.Type {
	case protocol.TypeCreateChat:
		c, err := g.chats.Create(ctx, s.user, f.Participants, f.Name)
		resp.ChatID = c.ID
		return err

	case protocol.TypeSend:
		if f.ChatID == "" || f.ClientMsgID == "" || len(f.ClientMsgID) > 64 || f.Message == "" || utf8.RuneCountInString(f.Message) > maxMessageRunes {
			return domain.ErrInvalid
		}
		req := pubsub.SendRequest{ChatID: f.ChatID, SenderID: s.user, ClientMsgID: f.ClientMsgID, Body: f.Message}
		var m domain.Message
		err := g.router.Do(ctx, domain.ChatTopic(f.ChatID), func(srv *pubsub.Server) (err error) {
			m, err = srv.Send(ctx, req)
			return err
		})
		resp.Seq = m.Seq
		return err

	case protocol.TypeModify:
		op := domain.Op(f.Operation)
		if f.ChatID == "" || f.UserID == "" || f.ClientMsgID == "" || (op != domain.OpAdd && op != domain.OpRemove) {
			return domain.ErrInvalid
		}
		req := pubsub.ModifyRequest{ChatID: f.ChatID, ActorID: s.user, TargetID: f.UserID, Op: op, ClientMsgID: f.ClientMsgID}
		var m domain.Message
		err := g.router.Do(ctx, domain.ChatTopic(f.ChatID), func(srv *pubsub.Server) (err error) {
			m, err = srv.Modify(ctx, req)
			return err
		})
		resp.Seq = m.Seq
		return err

	case protocol.TypeSync:
		if f.ChatID == "" || f.Seq < 0 {
			return domain.ErrInvalid
		}
		msgs, covered, more, err := g.store.Sync(ctx, f.ChatID, s.user, f.Seq, g.cfg.SyncLimit)
		if err != nil {
			return err
		}
		resp.Seq, resp.HasMore = covered, more
		resp.Messages = make([]protocol.Message, len(msgs))
		for i, m := range msgs {
			resp.Messages[i] = protocol.FromMessage(m)
		}
		return nil
	}
	return errors.New("unknown frame type")
}
