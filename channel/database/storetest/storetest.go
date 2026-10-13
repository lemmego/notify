// Package storetest is the contract every notification Store must satisfy.
//
// Two implementations held to one contract is the only cheap way to know that
// the memory store every application's handler tests run against behaves like
// the database every user actually hits. Run it from a store's own test file:
//
//	func TestMemoryStoreSatisfiesTheContract(t *testing.T) {
//	    storetest.Run(t, func(*testing.T) database.Store {
//	        return database.NewMemoryStore()
//	    })
//	}
//
// It is exported so a store written outside this module can prove itself too.
package storetest

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lemmego/notify/channel/database"
)

// Factory returns a fresh, empty store.
type Factory func(t *testing.T) database.Store

// Run executes the whole contract.
func Run(t *testing.T, newStore Factory) {
	t.Helper()

	cases := []struct {
		name string
		run  func(*testing.T, database.Store)
	}{
		{"InsertReportsWhatWasNew", insertReportsWhatWasNew},
		{"ADedupeKeyIsWrittenOnce", aDedupeKeyIsWrittenOnce},
		{"ConcurrentInsertsOfOneKeyWriteOneRow", concurrentInsertsOfOneKeyWriteOneRow},
		{"AnEmptyDedupeKeyNeverCollides", anEmptyDedupeKeyNeverCollides},
		{"DataSurvivesTheRoundTrip", dataSurvivesTheRoundTrip},
		{"PageIsNewestFirst", pageIsNewestFirst},
		{"PagingIsStableWhileRowsArrive", pagingIsStableWhileRowsArrive},
		{"PageOnlyShowsTheOwnersRows", pageOnlyShowsTheOwnersRows},
		{"UnreadStopsAtTheCeiling", unreadStopsAtTheCeiling},
		{"MarkReadIsIdempotent", markReadIsIdempotent},
		{"OneOwnerCannotTouchAnothersRows", oneOwnerCannotTouchAnothersRows},
		{"MarkAllReadCountsWhatItChanged", markAllReadCountsWhatItChanged},
		{"PruneTakesOnlyOldReadRows", pruneTakesOnlyOldReadRows},
	}

	for _, one := range cases {
		t.Run(one.name, func(t *testing.T) { one.run(t, newStore(t)) })
	}
}

var (
	ada   = database.Owner{Kind: "user", ID: "1"}
	grace = database.Owner{Kind: "user", ID: "2"}
)

func row(owner database.Owner, body string) database.Row {
	return database.Row{
		Owner:     owner,
		Type:      "followed",
		Data:      json.RawMessage(fmt.Sprintf(`{"body":%q}`, body)),
		CreatedAt: time.Now().UTC(),
	}
}

func insertReportsWhatWasNew(t *testing.T, store database.Store) {
	ctx := context.Background()
	created, err := store.Insert(ctx, []database.Row{row(ada, "a"), row(ada, "b")})
	if err != nil {
		t.Fatalf("inserting: %v", err)
	}
	if created != 2 {
		t.Fatalf("reported %d created, want 2", created)
	}
}

func aDedupeKeyIsWrittenOnce(t *testing.T, store database.Store) {
	ctx := context.Background()
	first := row(ada, "a")
	first.DedupeKey = "follow:7"
	second := row(ada, "a-again")
	second.DedupeKey = "follow:7"

	if _, err := store.Insert(ctx, []database.Row{first}); err != nil {
		t.Fatalf("inserting: %v", err)
	}
	created, err := store.Insert(ctx, []database.Row{second})
	if err != nil {
		t.Fatalf("inserting again: %v", err)
	}
	if created != 0 {
		t.Fatalf("reported %d created, want 0 — a repeated dedupe key must not "+
			"notify anybody a second time", created)
	}

	// And the same key for a different owner is a different notification.
	forGrace := row(grace, "a")
	forGrace.DedupeKey = "follow:7"
	created, err = store.Insert(ctx, []database.Row{forGrace})
	if err != nil {
		t.Fatalf("inserting for another owner: %v", err)
	}
	if created != 1 {
		t.Fatal("a dedupe key collided across owners, so one person's " +
			"notification suppressed another's")
	}
}

// concurrentInsertsOfOneKeyWriteOneRow is the case that tells a real unique
// index from a check-then-insert. Ten goroutines, one key, one row.
func concurrentInsertsOfOneKeyWriteOneRow(t *testing.T, store database.Store) {
	ctx := context.Background()

	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			one := row(ada, "a")
			one.DedupeKey = "follow:7"
			created, err := store.Insert(ctx, []database.Row{one})
			if err != nil {
				return
			}
			mu.Lock()
			total += created
			mu.Unlock()
		}()
	}
	wg.Wait()

	if total != 1 {
		t.Fatalf("ten concurrent inserts of one key created %d rows, want 1", total)
	}
}

func anEmptyDedupeKeyNeverCollides(t *testing.T, store database.Store) {
	ctx := context.Background()
	created, err := store.Insert(ctx, []database.Row{
		row(ada, "a"), row(ada, "b"), row(ada, "c"),
	})
	if err != nil {
		t.Fatalf("inserting: %v", err)
	}
	if created != 3 {
		t.Fatalf("reported %d created, want 3 — rows with no dedupe key must "+
			"not collide with each other", created)
	}
}

// dataSurvivesTheRoundTrip asserts the payload comes back meaning the same
// thing — not byte-identical.
//
// The byte-identical version of this test failed on Postgres, and the test
// was wrong rather than the store: jsonb stores a parsed binary form, so it
// sorts object keys and normalises whitespace by definition. That is the same
// property that makes it indexable, which is why the column is jsonb and not
// json.
//
// Nothing is lost by weakening it, because key order was never a guarantee a
// client could use: JSON objects are unordered by specification and the
// browser does JSON.parse. What must survive is every key and every value,
// nested ones included, and numbers without precision loss — so that is what
// is asserted. The one real consequence, for the doc rather than the test: a
// payload with duplicate keys is collapsed, so do not build one.
func dataSurvivesTheRoundTrip(t *testing.T, store database.Store) {
	ctx := context.Background()
	payload := json.RawMessage(
		`{"zebra":1,"actor":{"id":7,"handle":"ada"},"apple":2,"big":9007199254740991,"ratio":0.25,"flag":true,"nothing":null}`)
	one := row(ada, "x")
	one.Data = payload
	if _, err := store.Insert(ctx, []database.Row{one}); err != nil {
		t.Fatalf("inserting: %v", err)
	}

	rows, _, err := store.Page(ctx, ada, database.Cursor{})
	if err != nil {
		t.Fatalf("paging: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}

	// Decoded with numbers kept as json.Number, so a large integer that a
	// float64 could not hold is compared as it was written rather than as
	// 9.007199254740992e+15.
	want := decodeExactly(t, payload)
	got := decodeExactly(t, rows[0].Data)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("data came back as %s, want the same content as %s",
			rows[0].Data, payload)
	}
}

func decodeExactly(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var out any
	if err := decoder.Decode(&out); err != nil {
		t.Fatalf("decoding %s: %v", raw, err)
	}
	return out
}

func pageIsNewestFirst(t *testing.T, store database.Store) {
	ctx := context.Background()
	for _, body := range []string{"first", "second", "third"} {
		if _, err := store.Insert(ctx, []database.Row{row(ada, body)}); err != nil {
			t.Fatalf("inserting: %v", err)
		}
	}

	rows, _, err := store.Page(ctx, ada, database.Cursor{})
	if err != nil {
		t.Fatalf("paging: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	if !strings.Contains(string(rows[0].Data), "third") {
		t.Fatalf("the newest row is %s, want the one inserted last", rows[0].Data)
	}
}

// pagingIsStableWhileRowsArrive is the case an offset implementation fails.
func pagingIsStableWhileRowsArrive(t *testing.T, store database.Store) {
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := store.Insert(ctx, []database.Row{row(ada, fmt.Sprintf("old-%d", i))}); err != nil {
			t.Fatalf("inserting: %v", err)
		}
	}

	first, next, err := store.Page(ctx, ada, database.Cursor{Limit: 2})
	if err != nil {
		t.Fatalf("paging: %v", err)
	}
	if next.BeforeID == "" {
		t.Fatal("got no next cursor with more rows outstanding")
	}

	// Two more arrive at the top between the pages, which is the normal case
	// for an inbox and the one that breaks an offset.
	if _, err := store.Insert(ctx, []database.Row{row(ada, "new-a"), row(ada, "new-b")}); err != nil {
		t.Fatalf("inserting: %v", err)
	}

	second, _, err := store.Page(ctx, ada, next)
	if err != nil {
		t.Fatalf("paging the second page: %v", err)
	}

	seen := map[string]bool{}
	for _, r := range append(append([]database.Row{}, first...), second...) {
		if seen[r.ID] {
			t.Fatalf("row %s appeared on both pages", r.ID)
		}
		seen[r.ID] = true
	}
	for _, r := range second {
		if strings.Contains(string(r.Data), "new-") {
			t.Fatal("a row inserted after the first page appeared on the second, " +
				"so the cursor is an offset rather than a keyset")
		}
	}
}

func pageOnlyShowsTheOwnersRows(t *testing.T, store database.Store) {
	ctx := context.Background()
	if _, err := store.Insert(ctx, []database.Row{row(ada, "mine"), row(grace, "hers")}); err != nil {
		t.Fatalf("inserting: %v", err)
	}

	rows, _, err := store.Page(ctx, ada, database.Cursor{})
	if err != nil {
		t.Fatalf("paging: %v", err)
	}
	if len(rows) != 1 || !strings.Contains(string(rows[0].Data), "mine") {
		t.Fatalf("got %d rows, want only the owner's", len(rows))
	}
}

func unreadStopsAtTheCeiling(t *testing.T, store database.Store) {
	ctx := context.Background()
	batch := make([]database.Row, 0, 150)
	for i := 0; i < 150; i++ {
		batch = append(batch, row(ada, fmt.Sprintf("n-%d", i)))
	}
	if _, err := store.Insert(ctx, batch); err != nil {
		t.Fatalf("inserting: %v", err)
	}

	count, err := store.Unread(ctx, ada)
	if err != nil {
		t.Fatalf("counting: %v", err)
	}
	// MaxUnread+1, so a caller can tell "exactly 99" from "at least 99" and
	// render "99+" — and so the query is bounded however busy the account.
	if count != database.MaxUnread+1 {
		t.Fatalf("counted %d, want %d", count, database.MaxUnread+1)
	}
}

func markReadIsIdempotent(t *testing.T, store database.Store) {
	ctx := context.Background()
	if _, err := store.Insert(ctx, []database.Row{row(ada, "a")}); err != nil {
		t.Fatalf("inserting: %v", err)
	}
	rows, _, _ := store.Page(ctx, ada, database.Cursor{})
	id := rows[0].ID

	changed, err := store.MarkRead(ctx, ada, id, time.Now())
	if err != nil {
		t.Fatalf("marking read: %v", err)
	}
	if !changed {
		t.Fatal("the first MarkRead reported no change")
	}

	changed, err = store.MarkRead(ctx, ada, id, time.Now())
	if err != nil {
		t.Fatalf("marking read again: %v", err)
	}
	if changed {
		t.Fatal("the second MarkRead reported a change, so two taps on one " +
			"bell would both decrement a counter")
	}

	count, _ := store.Unread(ctx, ada)
	if count != 0 {
		t.Fatalf("still %d unread after marking the only row read", count)
	}
}

func oneOwnerCannotTouchAnothersRows(t *testing.T, store database.Store) {
	ctx := context.Background()
	if _, err := store.Insert(ctx, []database.Row{row(ada, "hers")}); err != nil {
		t.Fatalf("inserting: %v", err)
	}
	rows, _, _ := store.Page(ctx, ada, database.Cursor{})
	id := rows[0].ID

	// Not an error and not a success: the same answer as "no such row",
	// because a difference would tell Grace that Ada's notification exists.
	changed, err := store.MarkRead(ctx, grace, id, time.Now())
	if err != nil {
		t.Fatalf("got an error, want (false, nil): %v", err)
	}
	if changed {
		t.Fatal("another owner marked this notification read")
	}

	deleted, err := store.Delete(ctx, grace, id)
	if err != nil {
		t.Fatalf("got an error, want (false, nil): %v", err)
	}
	if deleted {
		t.Fatal("another owner deleted this notification")
	}

	if count, _ := store.Unread(ctx, ada); count != 1 {
		t.Fatalf("the owner's row was affected: %d unread, want 1", count)
	}
}

func markAllReadCountsWhatItChanged(t *testing.T, store database.Store) {
	ctx := context.Background()
	if _, err := store.Insert(ctx, []database.Row{
		row(ada, "a"), row(ada, "b"), row(grace, "hers"),
	}); err != nil {
		t.Fatalf("inserting: %v", err)
	}

	changed, err := store.MarkAllRead(ctx, ada, time.Now())
	if err != nil {
		t.Fatalf("marking all read: %v", err)
	}
	if changed != 2 {
		t.Fatalf("changed %d, want 2", changed)
	}

	// Already read, so nothing to change.
	changed, err = store.MarkAllRead(ctx, ada, time.Now())
	if err != nil {
		t.Fatalf("marking all read again: %v", err)
	}
	if changed != 0 {
		t.Fatalf("changed %d on the second call, want 0", changed)
	}

	if count, _ := store.Unread(ctx, grace); count != 1 {
		t.Fatalf("another owner's rows were marked read: %d unread, want 1", count)
	}
}

func pruneTakesOnlyOldReadRows(t *testing.T, store database.Store) {
	ctx := context.Background()
	old := time.Now().UTC().Add(-48 * time.Hour)
	cutoff := time.Now().UTC().Add(-24 * time.Hour)

	oldRead := row(ada, "old-read")
	oldRead.CreatedAt = old
	oldUnread := row(ada, "old-unread")
	oldUnread.CreatedAt = old
	recent := row(ada, "recent")

	if _, err := store.Insert(ctx, []database.Row{oldRead, oldUnread, recent}); err != nil {
		t.Fatalf("inserting: %v", err)
	}

	rows, _, _ := store.Page(ctx, ada, database.Cursor{})
	for _, r := range rows {
		if strings.Contains(string(r.Data), "old-read") {
			if _, err := store.MarkRead(ctx, ada, r.ID, time.Now()); err != nil {
				t.Fatalf("marking read: %v", err)
			}
		}
	}

	removed, err := store.Prune(ctx, cutoff, 100)
	if err != nil {
		t.Fatalf("pruning: %v", err)
	}
	if removed != 1 {
		t.Fatalf("pruned %d rows, want 1 — an unread row must survive at any age",
			removed)
	}

	rows, _, _ = store.Page(ctx, ada, database.Cursor{})
	if len(rows) != 2 {
		t.Fatalf("%d rows left, want 2", len(rows))
	}
	for _, r := range rows {
		if strings.Contains(string(r.Data), "old-read") {
			t.Fatal("the old read row survived the prune")
		}
	}
}
