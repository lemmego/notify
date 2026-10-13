package database

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrNotFound reports that a notification is not there — or is not this
// owner's, which is deliberately the same answer. See MarkRead.
var ErrNotFound = errors.New("notify/database: no such notification")

// MaxUnread is how far Unread counts before it stops.
//
// The badge renders "99+" past this, so counting further is work nobody sees.
// It is what makes Unread a bounded query forever rather than one that
// degrades with a busy account: a store reads at most MaxUnread+1 rows
// whether the owner has three unread or three hundred thousand. The +1 is so
// a caller can tell "exactly 99" from "at least 99".
const MaxUnread = 99

// Owner is who a notification belongs to: a notifiable reduced to its key.
//
// The store takes this rather than a notify.Notifiable so a storage
// implementation never needs an application's types.
type Owner struct{ Kind, ID string }

// Row is one stored notification.
type Row struct {
	// ID is assigned by the store on insert and ignored if supplied.
	ID    string
	Owner Owner

	// Type is the discriminator the client switches on. It comes from
	// Storable.NotificationType, never from the Go type name.
	Type string

	// Data is the payload as JSON. It round-trips byte-identically: a store
	// that re-marshals it may reorder keys, and the JSON is a contract with
	// the frontend.
	Data json.RawMessage

	// DedupeKey, when set, makes this row unique per owner. Empty means no
	// deduplication, and empty keys never collide with each other.
	DedupeKey string

	ReadAt    *time.Time
	CreatedAt time.Time
}

// DataAs decodes this row's payload.
//
// A method with its own type parameter, which this Go allows on a concrete
// type — so a caller writes what it means:
//
//	payload, err := row.DataAs[FollowedPayload]()
//
// rather than declaring a variable and remembering to pass its address.
// It is the shape cache's GetAs uses, and the reason Row is a struct: a
// generic method cannot exist on an interface.
//
// Numbers decode as json.Number, so a large integer that a float64 could
// not hold survives — which it would not through a map[string]any, where
// 9007199254740993 comes back as 9007199254740992.
func (r Row) DataAs[T any]() (T, error) {
	var out T
	if len(r.Data) == 0 {
		return out, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(r.Data))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return out, fmt.Errorf("notify/database: decoding row %s (%s): %w",
			r.ID, r.Type, err)
	}
	return out, nil
}

// Cursor pages an inbox.
//
// Keyset, not offset: rows arrive at the top of this list continuously, and
// under exactly that traffic an offset page shows one notification twice and
// skips another.
type Cursor struct {
	// BeforeID returns rows older than this one. Empty starts at the newest.
	BeforeID string

	// Limit is how many rows to return. Zero means DefaultPageSize.
	Limit int
}

// DefaultPageSize is the page size when a Cursor asks for none.
const DefaultPageSize = 20

// MaxPageSize caps what a caller may ask for, because the limit usually
// arrives from a query string.
const MaxPageSize = 100

// Store persists in-app notifications.
//
// The methods are transitions rather than CRUD. Marking a notification read
// is one conditional update whose affected row count is the answer, so two
// taps on the same bell cannot both report that they changed something — and
// the owner is part of every condition, so one person cannot read, mark or
// delete another person's notification even if a handler forgets to check.
// Exposing Get and Update instead would let every caller re-implement that
// check, and one of them would forget.
//
// Ids are strings although the column is very likely an integer. The seam
// stays free of the key's type so a memory store, a future uuid column and an
// id arriving from an HTTP path all speak one interface; a store parses and
// validates its own.
type Store interface {
	// Insert writes rows and reports how many were new.
	//
	// It takes a slice because a notification fanned out to a circle is one
	// statement, not one per member. A row whose DedupeKey is already present
	// for the same owner is skipped rather than written twice, so a retried
	// request does not notify anybody again.
	Insert(ctx context.Context, rows []Row) (created int, err error)

	// Page returns one page of an owner's inbox, newest first, and the cursor
	// for the next page. A cursor whose BeforeID is empty means there is no
	// next page.
	Page(ctx context.Context, owner Owner, cur Cursor) (rows []Row, next Cursor, err error)

	// Unread counts the owner's unread notifications, stopping at
	// MaxUnread+1.
	Unread(ctx context.Context, owner Owner) (int, error)

	// MarkRead marks one notification read and reports whether it changed.
	//
	// False means it was already read, never existed, or is not this owner's.
	// The three are not distinguished on purpose: telling a caller which
	// would tell them whether somebody else's notification exists.
	MarkRead(ctx context.Context, owner Owner, id string, at time.Time) (bool, error)

	// MarkAllRead marks every unread notification read and returns how many
	// changed.
	MarkAllRead(ctx context.Context, owner Owner, at time.Time) (int, error)

	// Delete removes one of the owner's notifications.
	Delete(ctx context.Context, owner Owner, id string) (bool, error)

	// Prune removes read notifications created before the cutoff, at most
	// limit at a time, and returns how many went.
	//
	// Batched because a year of notifications does not delete inside one
	// statement anybody wants to wait for, and because the migration runner
	// holds its whole batch in one transaction with a thirty second timeout.
	// An unread row is never pruned, whatever its age.
	Prune(ctx context.Context, before time.Time, limit int) (int, error)
}

// ClampLimit applies DefaultPageSize and MaxPageSize.
//
// Exported because every Store implementation needs the same clamping, and
// because a limit that arrives from a query string is the normal case.
func ClampLimit(limit int) int {
	switch {
	case limit <= 0:
		return DefaultPageSize
	case limit > MaxPageSize:
		return MaxPageSize
	default:
		return limit
	}
}
