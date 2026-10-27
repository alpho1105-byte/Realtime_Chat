package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"realtime-chat/internal/app"
	"realtime-chat/internal/auth"
	"realtime-chat/internal/protocol"
)

type server struct {
	t      *testing.T
	url    string
	signer *auth.Signer
}

func newServer(t *testing.T) *server {
	t.Helper()
	opts := app.DefaultOptions(filepath.Join(t.TempDir(), "chat.db"))
	opts.Logger = slog.New(slog.DiscardHandler)
	opts.Shards, opts.PubSubServers, opts.Gateways = 8, 2, 2
	a, err := app.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	signer := auth.NewSigner([]byte("secret"))
	ts := httptest.NewServer(NewRouter(Deps{Signer: signer, Connector: a, Dev: true}))
	t.Cleanup(func() {
		ts.Close()
		cancel()
		time.Sleep(50 * time.Millisecond)
		a.Close()
	})
	return &server{t: t, url: "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws", signer: signer}
}

type conn struct {
	t  *testing.T
	ws *websocket.Conn
}

func (s *server) dial(user string) *conn {
	s.t.Helper()
	tok, _ := s.signer.Issue(user, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, s.url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + tok}}})
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { ws.CloseNow() })
	c := &conn{t: s.t, ws: ws}
	c.expect(func(f protocol.ServerFrame) bool { return f.Type == protocol.TypeChatList })
	return c
}

func (c *conn) send(f protocol.ClientFrame) {
	c.t.Helper()
	if err := wsjson.Write(context.Background(), c.ws, f); err != nil {
		c.t.Fatal(err)
	}
}

// expect 讀取 frame 直到符合條件；推播與回應的先後不固定，所以略過不相關的 frame。
func (c *conn) expect(match func(protocol.ServerFrame) bool) protocol.ServerFrame {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		var f protocol.ServerFrame
		if err := wsjson.Read(ctx, c.ws, &f); err != nil {
			c.t.Fatalf("waiting for frame: %v", err)
		}
		if match(f) {
			return f
		}
	}
}

func response(reqID string) func(protocol.ServerFrame) bool {
	return func(f protocol.ServerFrame) bool { return f.Type == protocol.TypeResponse && f.ReqID == reqID }
}

func TestUpgradeRequiresAToken(t *testing.T) {
	s := newServer(t)
	_, resp, err := websocket.Dial(context.Background(), s.url, nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
}

func TestChatOverWebSocket(t *testing.T) {
	s := newServer(t)
	alice, bob := s.dial("alice"), s.dial("bob")

	alice.send(protocol.ClientFrame{Type: protocol.TypeCreateChat, ReqID: "1", Name: "pair", Participants: []string{"bob"}})
	created := alice.expect(response("1"))
	if created.Status != protocol.StatusSuccess || created.ChatID == "" {
		t.Fatalf("createChat: %+v", created)
	}
	chat := created.ChatID
	bob.expect(func(f protocol.ServerFrame) bool { return f.Type == protocol.TypeChatAdded && f.ChatID == chat })
	alice.expect(func(f protocol.ServerFrame) bool { return f.Type == protocol.TypeChatAdded && f.ChatID == chat })

	alice.send(protocol.ClientFrame{Type: protocol.TypeSend, ReqID: "2", ChatID: chat, ClientMsgID: "m1", Message: "hi bob"})
	if r := alice.expect(response("2")); r.Status != protocol.StatusSuccess || r.Seq != 1 {
		t.Fatalf("send: %+v", r)
	}
	pushed := bob.expect(func(f protocol.ServerFrame) bool { return f.Type == protocol.TypeNewMessage })
	if m := pushed.Message; m.Seq != 1 || m.SenderID != "alice" || m.Body != "hi bob" {
		t.Fatalf("bob got %+v", m)
	}

	// 回應遺失時 client 會重送同一個 clientMsgId，server 回傳同一個 message id。
	alice.send(protocol.ClientFrame{Type: protocol.TypeSend, ReqID: "3", ChatID: chat, ClientMsgID: "m1", Message: "hi bob"})
	if r := alice.expect(response("3")); r.Status != protocol.StatusSuccess || r.Seq != 1 {
		t.Fatalf("retry: %+v", r)
	}

	bob.send(protocol.ClientFrame{Type: protocol.TypeSync, ReqID: "4", ChatID: chat})
	if r := bob.expect(response("4")); len(r.Messages) != 1 || r.Seq != 1 {
		t.Fatalf("sync: %+v", r)
	}

	mallory := s.dial("mallory")
	mallory.send(protocol.ClientFrame{Type: protocol.TypeSend, ReqID: "5", ChatID: chat, ClientMsgID: "x1", Message: "let me in"})
	if r := mallory.expect(response("5")); r.Status != protocol.StatusFailure {
		t.Fatalf("non-member send: %+v", r)
	}
	mallory.send(protocol.ClientFrame{Type: protocol.TypeSync, ReqID: "6", ChatID: chat})
	if r := mallory.expect(response("6")); r.Status != protocol.StatusFailure || len(r.Messages) != 0 {
		t.Fatalf("non-member sync: %+v", r)
	}
}
