// Package database stores notifications so an application can show them in
// its own interface.
//
// This is the channel the module exists for: a notification that is only an
// email is gone once it is read, and a notification that is only a broadcast
// never reaches somebody who was not looking. The row is the record; the
// other channels tell people it is there.
package database

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lemmego/notify"
)

// Name is what a notification's Via must return to select this channel.
//
// A constant rather than a bare string in the core, so a notification writes
// []string{database.Name} and a typo is a compile error.
const Name = "database"

// Storable is the optional interface a notification implements to be stored.
type Storable interface {
	// NotificationType is the discriminator written to the row, and the
	// string the client switches on to choose a renderer.
	//
	// Spelled out rather than derived from the Go type name, for two
	// reasons: renaming a Go type must not orphan every row already written,
	// and this string is an API the frontend depends on — deriving it from an
	// implementation detail makes a refactor a breaking change nobody sees
	// until the inbox renders blank rows.
	NotificationType() string

	// ToDatabase returns the payload, which the channel marshals to JSON.
	//
	// Return a struct, not a map. The JSON is a contract with the client, and
	// a map is how a field gets quietly renamed.
	ToDatabase(ctx context.Context, to notify.Notifiable) (any, error)
}

// Deduplicated is the optional interface a notification implements to be
// written at most once per key.
//
// The first defence against a duplicate notification belongs at the call
// site, which should fire only on a real state transition — an upsert that
// reports whether it inserted anything. This is the second, and it covers
// what a call site cannot: a retried job, a replayed request, and the call
// site somebody adds next year without reading the rule.
//
// Choose the key's granularity deliberately, because the failure is silent in
// the dangerous direction. An unbounded key makes a notification once-ever:
// with "follow:7:42", somebody who follows, unfollows and follows again is
// never announced a second time, and nothing will ever tell you. Include a
// period when the event can legitimately recur:
//
//	func (n Followed) DedupeKey(to notify.Notifiable) string {
//	    return fmt.Sprintf("follow:%d:%s", n.ActorID,
//	        time.Now().UTC().Format("2006-01-02"))
//	}
type Deduplicated interface {
	DedupeKey(to notify.Notifiable) string
}

// Channel writes notifications to a Store.
type Channel struct {
	store Store
	now   func() time.Time
}

// Option configures a Channel.
type Option func(*Channel)

// WithClock replaces the clock, for a test that needs to place rows in time.
func WithClock(now func() time.Time) Option {
	return func(c *Channel) { c.now = now }
}

// New builds the channel over a store.
func New(store Store, opts ...Option) *Channel {
	if store == nil {
		panic("notify/database: New needs a store")
	}
	c := &Channel{store: store, now: time.Now}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Channel) Name() string { return Name }

// Supports reports whether a notification can be stored, and what is missing
// when it cannot.
func (c *Channel) Supports(n notify.Notification) (bool, string) {
	if _, ok := n.(Storable); !ok {
		return false, "does not implement database.Storable " +
			"(missing NotificationType and ToDatabase)"
	}
	return true, ""
}

// Send stores one notification.
func (c *Channel) Send(ctx context.Context, to notify.Notifiable, n notify.Notification) error {
	row, err := c.row(ctx, to, n)
	if err != nil {
		return err
	}
	_, err = c.store.Insert(ctx, []Row{row})
	return err
}

// SendMany stores one notification for many recipients in one statement.
//
// This is what makes notify.SendTo to a circle one insert rather than one per
// member. A recipient whose row cannot be built fails the whole call rather
// than being skipped: a partial fan-out that reports success is a set of
// people who were silently left out.
func (c *Channel) SendMany(ctx context.Context, recipients []notify.Notifiable, n notify.Notification) error {
	rows := make([]Row, 0, len(recipients))
	for _, to := range recipients {
		row, err := c.row(ctx, to, n)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil
	}
	_, err := c.store.Insert(ctx, rows)
	return err
}

// Store exposes the store, for the read side.
func (c *Channel) Store() Store { return c.store }

func (c *Channel) row(ctx context.Context, to notify.Notifiable, n notify.Notification) (Row, error) {
	storable, ok := n.(Storable)
	if !ok {
		return Row{}, fmt.Errorf("%w: %T does not implement database.Storable "+
			"(missing NotificationType and ToDatabase)", notify.ErrUnsupported, n)
	}

	kind, id := to.NotifiableKey()
	if kind == "" || id == "" {
		return Row{}, fmt.Errorf("%w: %T returned an empty notifiable key",
			notify.ErrNoRoute, to)
	}

	notificationType := storable.NotificationType()
	if notificationType == "" {
		return Row{}, fmt.Errorf("%w: %T returned an empty NotificationType, "+
			"which the client has nothing to switch on", notify.ErrUnsupported, n)
	}

	payload, err := storable.ToDatabase(ctx, to)
	if err != nil {
		return Row{}, fmt.Errorf("building the payload for %T: %w", n, err)
	}
	// Marshalled here rather than in the store, so a payload that cannot be
	// encoded is reported to the one caller who can fix it instead of once
	// per storage backend.
	data, err := json.Marshal(payload)
	if err != nil {
		return Row{}, fmt.Errorf("encoding the payload for %T: %w", n, err)
	}

	row := Row{
		Owner:     Owner{Kind: kind, ID: id},
		Type:      notificationType,
		Data:      data,
		CreatedAt: c.now().UTC(),
	}
	if deduplicated, ok := n.(Deduplicated); ok {
		row.DedupeKey = deduplicated.DedupeKey(to)
	}
	return row, nil
}
