package pubsub

import (
	"context"
	"errors"
	"time"

	"realtime-chat/internal/domain"
	"realtime-chat/internal/store"
)

// SendRequest 是一則文字訊息；ModifyRequest 是成員異動。兩者都會成為 chat 日誌裡的一筆紀錄。
type SendRequest struct {
	ChatID      string
	SenderID    string
	ClientMsgID string
	Body        string
}

type ModifyRequest struct {
	ChatID      string
	ActorID     string
	TargetID    string
	Op          domain.Op
	ClientMsgID string
}

type request struct {
	shard int
	kind  domain.Kind
	msg   domain.Message
	resp  chan result
}

type result struct {
	msg domain.Message
	dup bool
	err error
}

func (s *Server) Send(ctx context.Context, r SendRequest) (domain.Message, error) {
	return s.submit(ctx, &request{kind: domain.KindText, msg: domain.Message{ChatID: r.ChatID, SenderID: r.SenderID,
		ClientMsgID: r.ClientMsgID, Kind: domain.KindText, Body: r.Body}})
}

func (s *Server) Modify(ctx context.Context, r ModifyRequest) (domain.Message, error) {
	kind := domain.KindMemberAdded
	if r.Op == domain.OpRemove {
		kind = domain.KindMemberRemoved
	}
	return s.submit(ctx, &request{kind: kind, msg: domain.Message{ChatID: r.ChatID, SenderID: r.ActorID,
		ClientMsgID: r.ClientMsgID, Kind: kind, Target: r.TargetID}})
}

func (s *Server) submit(ctx context.Context, req *request) (domain.Message, error) {
	if !s.alive() {
		return domain.Message{}, domain.ErrUnavailable
	}
	req.shard = s.shardOf(domain.ChatTopic(req.msg.ChatID))
	if !s.owns(domain.ChatTopic(req.msg.ChatID)) {
		return domain.Message{}, domain.ErrNotOwner
	}
	req.resp = make(chan result, 1)
	select {
	case s.reqs <- req:
	case <-s.ctx.Done():
		return domain.Message{}, domain.ErrUnavailable
	case <-ctx.Done():
		return domain.Message{}, ctx.Err()
	}
	select {
	case r := <-req.resp:
		return r.msg, r.err
	case <-s.ctx.Done():
		return domain.Message{}, domain.ErrUnavailable
	case <-ctx.Done():
		return domain.Message{}, ctx.Err()
	}
}

// writeLoop 是 server 唯一的寫入者：一次取出佇列裡所有等待中的請求，在同一個交易裡寫入。
// 訊息越多、批次越大，fsync 次數不會跟著訊息數成長。
func (s *Server) writeLoop() {
	for {
		var batch []*request
		select {
		case <-s.ctx.Done():
			return
		case r := <-s.reqs:
			batch = append(batch, r)
		}
	drain:
		for len(batch) < s.cfg.MaxBatch {
			select {
			case r := <-s.reqs:
				batch = append(batch, r)
			default:
				break drain
			}
		}
		s.process(batch)
	}
}

type group struct {
	epoch  int64
	reqs   []*request
	fenced bool
}

func (s *Server) process(batch []*request) {
	s.batches.Add(1)
	groups := make(map[int]*group)
	s.mu.Lock()
	for _, r := range batch {
		epoch, ok := s.owned[r.shard]
		if !ok {
			r.resp <- result{err: domain.ErrNotOwner}
			continue
		}
		if groups[r.shard] == nil {
			groups[r.shard] = &group{epoch: epoch}
		}
		groups[r.shard].reqs = append(groups[r.shard].reqs, r)
	}
	s.mu.Unlock()

	// 已經決定好要寫什麼、還沒寫進去的這一刻停頓，正是 zombie owner 最危險的時候。
	s.waitIfPaused()

	results := make(map[*request]result, len(batch))
	next := make(map[string]int64)
	err := s.store.Write(s.ctx, func(tx *store.Tx) error {
		for shard, g := range groups {
			if err := tx.Fence(shard, g.epoch); err != nil {
				if errors.Is(err, domain.ErrFenced) {
					g.fenced = true
					continue
				}
				return err
			}
			for _, r := range g.reqs {
				res, err := s.apply(tx, r, next)
				if err != nil {
					return err
				}
				results[r] = res
			}
		}
		return nil
	})
	if err != nil {
		if s.ctx.Err() == nil {
			s.log.Error("write batch", "server", s.id, "err", err)
		}
		for _, g := range groups {
			for _, r := range g.reqs {
				r.resp <- result{err: domain.ErrUnavailable}
			}
		}
		return
	}

	s.mu.Lock()
	for shard, g := range groups {
		if g.fenced {
			// 儲存層已經有更新的 epoch：這個 shard 已經交給別人，這批請求一個都沒寫進去。
			// 停頓期間這台 server 可能已經重新加入、用新的 epoch 接回同一個 shard，那份 ownership 不能放掉。
			s.fenced.Add(int64(len(g.reqs)))
			if s.owned[shard] == g.epoch {
				s.dropShardLocked(shard)
			}
			for _, r := range g.reqs {
				r.resp <- result{err: domain.ErrNotOwner}
			}
			continue
		}
		// commit 之後 shard 可能已經被 watch loop 放掉；這時訊息已經安全寫入，
		// 只是不再由這裡推播，訂閱者改向新 owner 訂閱時會收到 resync。
		stillOwner := s.owned[shard] == g.epoch
		for _, r := range g.reqs {
			res := results[r]
			switch {
			case res.dup:
				s.duplicates.Add(1)
			case res.err == nil:
				s.committed.Add(1)
				msg := res.msg
				if stillOwner {
					s.next[msg.ChatID] = msg.Seq + 1
					s.publishLocked(Event{Topic: domain.ChatTopic(msg.ChatID), Message: &msg})
				}
				if msg.Kind == domain.KindMemberAdded {
					go s.announceJoin(msg)
				}
			}
			r.resp <- res
		}
	}
	s.mu.Unlock()
}

// apply 替一個請求指派 seq 並寫入。違反規則（不是成員、聊天室已滿）只讓這個請求失敗；
// 回傳的 error 則代表資料庫出錯，整個交易要 rollback。
func (s *Server) apply(tx *store.Tx, r *request, next map[string]int64) (result, error) {
	m := r.msg
	if seq, found, err := tx.FindClientMsg(m.ChatID, m.SenderID, m.ClientMsgID); err != nil {
		return result{}, err
	} else if found {
		m.Seq = seq
		return result{msg: m, dup: true}, nil
	}
	member, err := tx.IsMember(m.ChatID, m.SenderID)
	if err != nil {
		return result{}, err
	}
	if !member {
		return result{err: domain.ErrNotMember}, nil
	}
	switch r.kind {
	case domain.KindMemberAdded:
		in, err := tx.IsMember(m.ChatID, m.Target)
		if err != nil {
			return result{}, err
		}
		if in {
			return result{err: domain.ErrAlreadyIn}, nil
		}
		n, err := tx.MemberCount(m.ChatID)
		if err != nil {
			return result{}, err
		}
		if n >= domain.MaxParticipants {
			return result{err: domain.ErrChatFull}, nil
		}
	case domain.KindMemberRemoved:
		in, err := tx.IsMember(m.ChatID, m.Target)
		if err != nil {
			return result{}, err
		}
		if !in {
			return result{err: domain.ErrNotMember}, nil
		}
	}

	seq, ok := next[m.ChatID]
	if !ok {
		s.mu.Lock()
		seq, ok = s.next[m.ChatID]
		s.mu.Unlock()
	}
	if !ok {
		// 剛接手這個 shard：從儲存層讀出最新的 seq。fence 已經立下，之後不會有別人再寫入。
		maxSeq, err := tx.MaxSeq(m.ChatID)
		if err != nil {
			return result{}, err
		}
		seq = maxSeq + 1
	}
	m.Seq, m.CreatedAt = seq, time.Now()
	if err := tx.Insert(m); err != nil {
		return result{}, err
	}
	switch r.kind {
	case domain.KindMemberAdded:
		err = tx.Join(m.ChatID, m.Target, seq)
	case domain.KindMemberRemoved:
		err = tx.Leave(m.ChatID, m.Target, seq)
	}
	if err != nil {
		return result{}, err
	}
	next[m.ChatID] = seq + 1
	return result{msg: m}, nil
}

// announceJoin 通知被加入的人：他的 gateway 收到後會訂閱這個 chat，client 再從加入的位置開始補訊息。
func (s *Server) announceJoin(m domain.Message) {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	c, err := s.store.Chat(ctx, m.ChatID)
	if err != nil {
		s.log.Error("load chat for join notice", "chat", m.ChatID, "err", err)
		return
	}
	ev := Event{ChatAdded: &domain.ChatState{ChatID: c.ID, Name: c.Name, Member: true}}
	if err := s.notify.NotifyUser(ctx, m.Target, ev); err != nil {
		s.log.Warn("join notice not delivered; the user will see the chat on reconnect", "chat", m.ChatID, "user", m.Target, "err", err)
	}
}
