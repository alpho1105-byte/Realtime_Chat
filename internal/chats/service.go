package chats

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"realtime-chat/internal/cluster"
	"realtime-chat/internal/domain"
	"realtime-chat/internal/pubsub"
	"realtime-chat/internal/store"
)

// Service 建立聊天室。建立時還沒有訊息，不需要經過 owner 排序；
// 之後的成員異動才是 chat 日誌的一部分，由 owner 處理。
type Service struct {
	store  *store.Store
	router *cluster.Router
	log    *slog.Logger
}

func NewService(st *store.Store, router *cluster.Router, log *slog.Logger) *Service {
	return &Service{store: st, router: router, log: log}
}

func (s *Service) Create(ctx context.Context, creator string, participants []string, name string) (domain.Chat, error) {
	name = strings.TrimSpace(name)
	if name == "" || utf8.RuneCountInString(name) > 100 {
		return domain.Chat{}, domain.ErrInvalid
	}
	members := []string{creator}
	seen := map[string]bool{creator: true}
	for _, p := range participants {
		if p == "" || len(p) > 64 {
			return domain.Chat{}, domain.ErrInvalid
		}
		if !seen[p] {
			seen[p] = true
			members = append(members, p)
		}
	}
	if len(members) < 2 {
		return domain.Chat{}, domain.ErrInvalid
	}
	if len(members) > domain.MaxParticipants {
		return domain.Chat{}, domain.ErrChatFull
	}

	c := domain.Chat{ID: newID(), Name: name, CreatedBy: creator, CreatedAt: time.Now()}
	if err := s.store.CreateChat(ctx, c, members); err != nil {
		return domain.Chat{}, err
	}
	for _, u := range members {
		go s.announce(c, u)
	}
	return c, nil
}

// announce 通知在線的成員：他們的 gateway 會訂閱這個 chat。離線的人下次連線時從 chat 清單看到它。
func (s *Service) announce(c domain.Chat, userID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ev := pubsub.Event{ChatAdded: &domain.ChatState{ChatID: c.ID, Name: c.Name, Member: true}}
	if err := s.router.NotifyUser(ctx, userID, ev); err != nil {
		s.log.Warn("new chat notice not delivered", "chat", c.ID, "user", userID, "err", err)
	}
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return "c" + hex.EncodeToString(b)
}
