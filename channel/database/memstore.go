package database

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"
)

// MemoryStore keeps notifications in memory.
//
// It lives in this package rather than in a subpackage because it is standard
// library only, so it costs no consumer anything, and because it is what
// every application's handler tests run against.
//
// It enforces deduplication and owner scoping exactly as the SQL store does.
// A memory store that is laxer than production is a memory store that makes
// the suite green and the deployment wrong, which is the whole reason
// storetest holds both to one contract.
type MemoryStore struct {
	mu   sync.RWMutex
	next int64
	rows []Row // newest last; Page reverses
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

var _ Store = (*MemoryStore)(nil)

func (s *MemoryStore) Insert(_ context.Context, rows []Row) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	created := 0
	for _, row := range rows {
		if row.DedupeKey != "" && s.hasKey(row.Owner, row.DedupeKey) {
			continue
		}
		s.next++
		row.ID = strconv.FormatInt(s.next, 10)
		if row.CreatedAt.IsZero() {
			row.CreatedAt = time.Now().UTC()
		}
		s.rows = append(s.rows, row)
		created++
	}
	return created, nil
}

// hasKey answers the dedupe question. Called with the lock held.
//
// Note it is checked inside the same critical section as the append, so two
// concurrent inserts of one key cannot both decide the key is absent. That
// matters: the SQL store gets this from a unique index, and a memory store
// that checked before taking the lock would be the laxer implementation this
// type exists not to be.
func (s *MemoryStore) hasKey(owner Owner, key string) bool {
	for _, row := range s.rows {
		if row.Owner == owner && row.DedupeKey == key {
			return true
		}
	}
	return false
}

func (s *MemoryStore) Page(_ context.Context, owner Owner, cur Cursor) ([]Row, Cursor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := ClampLimit(cur.Limit)

	// Newest first, and by id rather than by CreatedAt: two rows inserted in
	// one call share a timestamp, and a sort on it would order them
	// arbitrarily between calls.
	mine := make([]Row, 0, len(s.rows))
	for _, row := range s.rows {
		if row.Owner != owner {
			continue
		}
		mine = append(mine, row)
	}
	sort.Slice(mine, func(i, j int) bool {
		return numericID(mine[i].ID) > numericID(mine[j].ID)
	})

	if cur.BeforeID != "" {
		before := numericID(cur.BeforeID)
		filtered := mine[:0:0]
		for _, row := range mine {
			if numericID(row.ID) < before {
				filtered = append(filtered, row)
			}
		}
		mine = filtered
	}

	var next Cursor
	if len(mine) > limit {
		mine = mine[:limit]
		next = Cursor{BeforeID: mine[len(mine)-1].ID, Limit: limit}
	}
	return mine, next, nil
}

func (s *MemoryStore) Unread(_ context.Context, owner Owner) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, row := range s.rows {
		if row.Owner == owner && row.ReadAt == nil {
			count++
			// Stops at the ceiling, like the SQL store's LIMIT, so the two
			// agree on what "a lot" reports.
			if count > MaxUnread {
				break
			}
		}
	}
	return count, nil
}

func (s *MemoryStore) MarkRead(_ context.Context, owner Owner, id string, at time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.rows {
		// The owner is part of the condition, not checked by the caller.
		if s.rows[i].ID != id || s.rows[i].Owner != owner {
			continue
		}
		if s.rows[i].ReadAt != nil {
			return false, nil
		}
		when := at.UTC()
		s.rows[i].ReadAt = &when
		return true, nil
	}
	return false, nil
}

func (s *MemoryStore) MarkAllRead(_ context.Context, owner Owner, at time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	when := at.UTC()
	changed := 0
	for i := range s.rows {
		if s.rows[i].Owner != owner || s.rows[i].ReadAt != nil {
			continue
		}
		read := when
		s.rows[i].ReadAt = &read
		changed++
	}
	return changed, nil
}

func (s *MemoryStore) Delete(_ context.Context, owner Owner, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.rows {
		if s.rows[i].ID != id || s.rows[i].Owner != owner {
			continue
		}
		s.rows = append(s.rows[:i], s.rows[i+1:]...)
		return true, nil
	}
	return false, nil
}

func (s *MemoryStore) Prune(_ context.Context, before time.Time, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if limit <= 0 {
		limit = MaxPageSize
	}
	kept := make([]Row, 0, len(s.rows))
	removed := 0
	for _, row := range s.rows {
		prunable := row.ReadAt != nil && row.CreatedAt.Before(before)
		if prunable && removed < limit {
			removed++
			continue
		}
		kept = append(kept, row)
	}
	s.rows = kept
	return removed, nil
}

// Len reports how many rows are stored, for a test asserting on Prune.
func (s *MemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.rows)
}

// numericID parses an id for ordering, treating anything unparseable as 0.
//
// The ids this store issues are always decimal, so the fallback only matters
// for an id a caller invented — which Page should order consistently rather
// than panic on.
func numericID(id string) int64 {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
