package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"realtime-chat/internal/auth"
	"realtime-chat/internal/gateway"
	"realtime-chat/internal/protocol"
)

// Connector 把新連線交給一台 gateway（正式環境是 L4/L7 負載平衡器做的事）。
type Connector interface {
	Connect(ctx context.Context, userID string) (*gateway.Session, error)
}

type Deps struct {
	Signer    *auth.Signer
	Connector Connector
	// Dev 開啟 POST /dev/token，方便本機測試時取得 token。
	Dev bool
}

func NewRouter(d Deps) http.Handler {
	mux := http.NewServeMux()
	h := &handler{d: d}
	mux.HandleFunc("GET /ws", h.websocket)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	if d.Dev {
		mux.HandleFunc("POST /dev/token", h.token)
	}
	return mux
}

type handler struct {
	d Deps
}

// websocket 在 HTTP upgrade 時驗證身分。瀏覽器的 WebSocket API 不能自訂 header，
// 所以也接受放在 query string 的 token。
func (h *handler) websocket(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		token = r.URL.Query().Get("token")
	}
	user, err := h.d.Signer.Verify(token)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid or missing token"})
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(64 << 10)
	ctx := r.Context()
	sess, err := h.d.Connector.Connect(ctx, user)
	if err != nil {
		conn.Close(websocket.StatusTryAgainLater, "no gateway available")
		return
	}
	defer sess.Close()

	go func() {
		for f := range sess.Frames() {
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := wsjson.Write(wctx, conn, f)
			cancel()
			if err != nil {
				conn.CloseNow()
				return
			}
		}
		// gateway 結束了這條連線（例如 client 跟不上推播），請 client 重新連線。
		conn.Close(websocket.StatusTryAgainLater, "reconnect")
	}()

	for {
		var f protocol.ClientFrame
		if err := wsjson.Read(ctx, conn, &f); err != nil {
			return
		}
		if err := sess.Send(f); err != nil {
			return
		}
	}
}

func (h *handler) token(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.UserID) > 64 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "userId is required"})
		return
	}
	token, _ := h.d.Signer.Issue(req.UserID, 24*time.Hour)
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
