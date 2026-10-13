package database

import (
	"context"
	"fmt"
	"time"

	"github.com/lemmego/notify"
)

// Inbox is the read side of this channel.
//
// It takes a notify.Notifiable rather than an Owner, and that is the whole
// reason it exists. A handler holding the signed-in user passes it straight
// through; a handler that had to build an Owner could build one out of a
// route parameter, and an inbox showing somebody else's notifications is the
// one bug in this feature that matters. The store enforces owner scoping too,
// so this is the second lock rather than the only one — but it is the lock on
// the door people actually walk through.
type Inbox struct {
	store Store
	now   func() time.Time
}

// NewInbox builds the read side over a store.
func NewInbox(store Store) *Inbox {
	if store == nil {
		panic("notify/database: NewInbox needs a store")
	}
	return &Inbox{store: store, now: time.Now}
}

// Page returns one page of somebody's notifications, newest first.
func (i *Inbox) Page(ctx context.Context, to notify.Notifiable, cur Cursor) ([]Row, Cursor, error) {
	owner, err := ownerOf(to)
	if err != nil {
		return nil, Cursor{}, err
	}
	return i.store.Page(ctx, owner, cur)
}

// Unread counts somebody's unread notifications, stopping at MaxUnread+1.
//
// Cheap enough to call on every page render, and deliberately not cached: an
// unread badge is state, not discovery, and it is the thing the person just
// clicked. A badge still reading 3 after you have read all three is the most
// noticeable bug a notification system can have, and a cache builds it in on
// purpose.
func (i *Inbox) Unread(ctx context.Context, to notify.Notifiable) (int, error) {
	owner, err := ownerOf(to)
	if err != nil {
		return 0, err
	}
	return i.store.Unread(ctx, owner)
}

// MarkRead marks one notification read and reports whether it changed.
func (i *Inbox) MarkRead(ctx context.Context, to notify.Notifiable, id string) (bool, error) {
	owner, err := ownerOf(to)
	if err != nil {
		return false, err
	}
	return i.store.MarkRead(ctx, owner, id, i.now().UTC())
}

// MarkAllRead marks everything read and returns how many changed.
func (i *Inbox) MarkAllRead(ctx context.Context, to notify.Notifiable) (int, error) {
	owner, err := ownerOf(to)
	if err != nil {
		return 0, err
	}
	return i.store.MarkAllRead(ctx, owner, i.now().UTC())
}

// Delete removes one of somebody's notifications.
func (i *Inbox) Delete(ctx context.Context, to notify.Notifiable, id string) (bool, error) {
	owner, err := ownerOf(to)
	if err != nil {
		return false, err
	}
	return i.store.Delete(ctx, owner, id)
}

// ownerOf reduces a notifiable to its key, refusing an empty one.
//
// An empty key would match every row whose columns are also empty, which is
// not a theoretical set once anything else writes to this table.
func ownerOf(to notify.Notifiable) (Owner, error) {
	if to == nil {
		return Owner{}, fmt.Errorf("notify/database: no recipient")
	}
	kind, id := to.NotifiableKey()
	if kind == "" || id == "" {
		return Owner{}, fmt.Errorf("notify/database: %T returned an empty notifiable key", to)
	}
	return Owner{Kind: kind, ID: id}, nil
}
