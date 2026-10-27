package main

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
)

const (
	userCount = 5_000
	chatCount = 1_500
)

// chatPlan 是一個要建立的聊天室：建立者與初始成員。
type chatPlan struct {
	name    string
	creator int
	members []int
}

// planChats 產生聊天室：一半是一對一，其餘是大小不一的群組，少數接近 100 人上限。
func planChats(rng *rand.Rand) []chatPlan {
	var plans []chatPlan
	for i := range chatCount {
		var size int
		switch r := rng.Float64(); {
		case r < 0.50:
			size = 2
		case r < 0.85:
			size = 3 + rng.IntN(8)
		case r < 0.97:
			size = 11 + rng.IntN(30)
		default:
			size = 41 + rng.IntN(55)
		}
		perm := rng.Perm(userCount)[:size]
		plans = append(plans, chatPlan{name: fmt.Sprintf("chat %d", i), creator: perm[0], members: perm})
	}
	return plans
}

// roster 是模擬端記錄的成員名單，用來挑選使用者要在哪個聊天室發言、要加入或移除誰。
// 它只是「使用者以為自己在哪些聊天室」，正確答案永遠以儲存層的日誌為準。
type roster struct {
	mu      sync.Mutex
	members map[string]map[int]bool
	byUser  map[int][]string
}

func newRoster() *roster {
	return &roster{members: make(map[string]map[int]bool), byUser: make(map[int][]string)}
}

func (r *roster) set(chatID string, users []int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.members[chatID] = make(map[int]bool)
	for _, u := range users {
		r.members[chatID][u] = true
		r.byUser[u] = append(r.byUser[u], chatID)
	}
}

func (r *roster) add(chatID string, u int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.members[chatID][u] = true
	r.byUser[u] = append(r.byUser[u], chatID)
}

func (r *roster) remove(chatID string, u int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.members[chatID], u)
	r.byUser[u] = slices.DeleteFunc(r.byUser[u], func(id string) bool { return id == chatID })
}

func (r *roster) chatsOf(u int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.byUser[u])
}

func (r *roster) membersOf(chatID string) []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, 0, len(r.members[chatID]))
	for u := range r.members[chatID] {
		out = append(out, u)
	}
	slices.Sort(out)
	return out
}
