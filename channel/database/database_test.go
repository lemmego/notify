package database_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lemmego/notify"
	"github.com/lemmego/notify/channel/database"
)

type user struct{ id string }

func (u user) NotifiableKey() (string, string) { return "user", u.id }

type followed struct {
	ActorID string `json:"actor_id"`
	dedupe  string
}

func (followed) Via(notify.Notifiable) []string { return []string{database.Name} }
func (followed) NotificationType() string       { return "followed" }

func (f followed) ToDatabase(context.Context, notify.Notifiable) (any, error) {
	return map[string]string{"actor_id": f.ActorID}, nil
}

// unstorable names the database channel without implementing Storable, which
// Go cannot catch at compile time.
type unstorable struct{}

func (unstorable) Via(notify.Notifiable) []string { return []string{database.Name} }

func TestANotificationIsStored(t *testing.T) {
	store := database.NewMemoryStore()
	n := notify.New(database.New(store))

	if err := n.Send(context.Background(), user{"1"}, followed{ActorID: "7"}); err != nil {
		t.Fatalf("sending: %v", err)
	}

	rows, _, err := database.NewInbox(store).Page(context.Background(), user{"1"}, database.Cursor{})
	if err != nil {
		t.Fatalf("paging: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].Type != "followed" {
		t.Fatalf("got type %q, want followed", rows[0].Type)
	}
	var payload map[string]string
	if err := json.Unmarshal(rows[0].Data, &payload); err != nil {
		t.Fatalf("decoding the payload: %v", err)
	}
	if payload["actor_id"] != "7" {
		t.Fatalf("got payload %v, want actor_id 7", payload)
	}
}

func TestANotificationThatCannotBeStoredSaysWhatIsMissing(t *testing.T) {
	n := notify.New(database.New(database.NewMemoryStore()))

	err := n.Send(context.Background(), user{"1"}, unstorable{})
	if !errors.Is(err, notify.ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported", err)
	}
	// The message has to name the interface and the method, or the reader has
	// to go and read this package to find out what they owed.
	if !strings.Contains(err.Error(), "database.Storable") ||
		!strings.Contains(err.Error(), "ToDatabase") {
		t.Fatalf("the error does not say what is missing: %v", err)
	}
}

func TestSupportsAnswersWithoutStoringAnything(t *testing.T) {
	store := database.NewMemoryStore()
	n := notify.New(database.New(store))

	if err := n.Supports(user{"1"}, unstorable{}); err == nil {
		t.Fatal("Supports accepted a notification that cannot be stored")
	}
	if store.Len() != 0 {
		t.Fatal("Supports wrote a row; it must only ask")
	}
	if err := n.Supports(user{"1"}, followed{ActorID: "7"}); err != nil {
		t.Fatalf("Supports rejected a storable notification: %v", err)
	}
}

// An empty NotificationType is refused, because the client switches on it and
// a blank discriminator renders an empty row with nothing to explain it.
func TestAnEmptyNotificationTypeIsRefused(t *testing.T) {
	n := notify.New(database.New(database.NewMemoryStore()))

	err := n.Send(context.Background(), user{"1"}, typeless{})
	if !errors.Is(err, notify.ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported", err)
	}
}

type typeless struct{}

func (typeless) Via(notify.Notifiable) []string { return []string{database.Name} }
func (typeless) NotificationType() string       { return "" }
func (typeless) ToDatabase(context.Context, notify.Notifiable) (any, error) {
	return struct{}{}, nil
}

func TestAFanOutIsOneInsert(t *testing.T) {
	store := &countingStore{Store: database.NewMemoryStore()}
	n := notify.New(database.New(store))

	recipients := []notify.Notifiable{user{"1"}, user{"2"}, user{"3"}}
	if err := n.SendTo(context.Background(), recipients, followed{ActorID: "7"}); err != nil {
		t.Fatalf("sending: %v", err)
	}

	// Three rows, one statement. A circle of two hundred members is the case
	// this protects.
	if store.inserts != 1 {
		t.Fatalf("called Insert %d times, want 1", store.inserts)
	}
	for _, who := range []string{"1", "2", "3"} {
		count, err := database.NewInbox(store).Unread(context.Background(), user{who})
		if err != nil {
			t.Fatalf("counting for %s: %v", who, err)
		}
		if count != 1 {
			t.Fatalf("user %s has %d unread, want 1", who, count)
		}
	}
}

type countingStore struct {
	database.Store
	inserts int
}

func (s *countingStore) Insert(ctx context.Context, rows []database.Row) (int, error) {
	s.inserts++
	return s.Store.Insert(ctx, rows)
}

func TestADedupedNotificationIsWrittenOnce(t *testing.T) {
	store := database.NewMemoryStore()
	n := notify.New(database.New(store))

	for i := 0; i < 3; i++ {
		if err := n.Send(context.Background(), user{"1"}, deduped{}); err != nil {
			t.Fatalf("sending: %v", err)
		}
	}

	count, _ := database.NewInbox(store).Unread(context.Background(), user{"1"})
	if count != 1 {
		t.Fatalf("got %d notifications, want 1 — a dedupe key must survive a "+
			"replayed request", count)
	}
}

type deduped struct{ followed }

func (deduped) DedupeKey(notify.Notifiable) string { return "follow:7:2026-10-13" }

func TestTheInboxWillNotTakeAnEmptyRecipient(t *testing.T) {
	inbox := database.NewInbox(database.NewMemoryStore())

	// A handler that built its owner out of a route parameter would arrive
	// here with an empty key, and an empty key matches rows nothing owns.
	if _, err := inbox.Unread(context.Background(), user{""}); err == nil {
		t.Fatal("an empty notifiable key was accepted")
	}
	if _, _, err := inbox.Page(context.Background(), nil, database.Cursor{}); err == nil {
		t.Fatal("a nil recipient was accepted")
	}
}

func TestMarkAllReadClearsTheBadge(t *testing.T) {
	store := database.NewMemoryStore()
	n := notify.New(database.New(store))
	inbox := database.NewInbox(store)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := n.Send(ctx, user{"1"}, followed{ActorID: string(rune('a' + i))}); err != nil {
			t.Fatalf("sending: %v", err)
		}
	}

	changed, err := inbox.MarkAllRead(ctx, user{"1"})
	if err != nil {
		t.Fatalf("marking all read: %v", err)
	}
	if changed != 3 {
		t.Fatalf("changed %d, want 3", changed)
	}
	count, _ := inbox.Unread(ctx, user{"1"})
	if count != 0 {
		t.Fatalf("badge still reads %d after marking everything read", count)
	}
	_ = time.Now
}
