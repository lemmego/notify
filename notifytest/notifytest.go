// Package notifytest helps an application test its own notifications.
//
// Two things live here. Contract is the one an application owes itself the
// moment it defines a second notification: Via returns strings, so the
// pairing of a notification with a channel is only checked when it is sent,
// and one table-driven test moves that check to the only place it is cheap.
// Recorder is a channel that records instead of delivering, for asserting
// that a handler notified the right person.
package notifytest

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/lemmego/notify"
)

// User is a minimal notifiable, for a test that does not need a real one.
//
// It implements notify.Notifiable and nothing else, deliberately: a channel
// that needs an email address will report ErrNoRoute for it, which is the
// correct answer and keeps this type from quietly importing lemmego/mail
// into every test that wants a recipient.
type User struct {
	Kind string
	ID   string
}

func (u User) NotifiableKey() (string, string) {
	kind := u.Kind
	if kind == "" {
		kind = "user"
	}
	id := u.ID
	if id == "" {
		id = "1"
	}
	return kind, id
}

// Contract asserts that every notification can be delivered on every channel
// its Via names for this recipient.
//
// Without it, the first time anybody learns that Replied forgot ToMail is
// when somebody replies.
//
//	func TestNotificationsSatisfyTheirChannels(t *testing.T) {
//	    notifytest.Contract(t,
//	        notify.New(database.New(store), mailchan.New(mailer)),
//	        notifytest.User{ID: "1"},
//	        notifications.All()...,
//	    )
//	}
//
// Two limits worth knowing. Via takes the recipient, so this can only cover
// the recipients it is handed — once preferences exist, pass one recipient
// per preference shape with ContractFor. And a notification missing from the
// list passed here is not checked, which is why that list belongs in the same
// package as the notifications themselves, where a reviewer sees both.
func Contract(t testing.TB, n *notify.Notifier, to notify.Notifiable, all ...notify.Notification) {
	t.Helper()
	ContractFor(t, n, []notify.Notifiable{to}, all...)
}

// ContractFor is Contract across several recipients.
func ContractFor(t testing.TB, n *notify.Notifier, recipients []notify.Notifiable, all ...notify.Notification) {
	t.Helper()

	if len(all) == 0 {
		t.Fatal("notifytest: Contract was given no notifications. An empty " +
			"list passes vacuously, which is worse than no test — pass the " +
			"list your notifications package exports.")
	}
	if len(recipients) == 0 {
		t.Fatal("notifytest: Contract was given no recipients")
	}

	for _, notification := range all {
		if notification == nil {
			t.Error("notifytest: a nil notification is in the list")
			continue
		}
		for _, to := range recipients {
			if err := n.Supports(to, notification); err != nil {
				kind, id := to.NotifiableKey()
				t.Errorf("%T cannot be delivered to %s:%s — %v",
					notification, kind, id, err)
			}
		}
	}
}

// Recorder is a channel that records what it was asked to deliver.
//
// It carries a name so it can stand in for any channel: give it
// database.Name and a handler test can assert what would have been stored
// without a database.
type Recorder struct {
	name string

	mu       sync.Mutex
	records  []Record
	err      error
	supports func(notify.Notification) (bool, string)
}

// Record is one delivery the Recorder was asked to make.
type Record struct {
	Channel      string
	Notification notify.Notification
	// Type is the notification's Go type, for an assertion that does not want
	// to name the type.
	Type      string
	OwnerKind string
	OwnerID   string
}

// NewRecorder returns a recorder standing in for the named channel.
func NewRecorder(name string) *Recorder {
	if name == "" {
		panic("notifytest: NewRecorder needs a channel name")
	}
	return &Recorder{name: name}
}

func (r *Recorder) Name() string { return r.name }

// Fail makes every later delivery return err, for testing what a handler does
// when a channel is down.
func (r *Recorder) Fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

// Refuse makes this recorder answer notify.Supports in the negative, for
// testing that Contract catches a mismatch.
func (r *Recorder) Refuse(missing string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.supports = func(notify.Notification) (bool, string) { return false, missing }
}

func (r *Recorder) Supports(n notify.Notification) (bool, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.supports == nil {
		return true, ""
	}
	return r.supports(n)
}

func (r *Recorder) Send(_ context.Context, to notify.Notifiable, n notify.Notification) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	kind, id := to.NotifiableKey()
	r.records = append(r.records, Record{
		Channel:      r.name,
		Notification: n,
		Type:         fmt.Sprintf("%T", n),
		OwnerKind:    kind,
		OwnerID:      id,
	})
	return nil
}

// Records returns every delivery, in order.
func (r *Recorder) Records() []Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Record(nil), r.records...)
}

// Count reports how many deliveries were made.
func (r *Recorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

// Reset forgets everything recorded.
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = nil
}

// For returns the deliveries made to one recipient.
func (r *Recorder) For(to notify.Notifiable) []Record {
	kind, id := to.NotifiableKey()
	out := []Record{}
	for _, record := range r.Records() {
		if record.OwnerKind == kind && record.OwnerID == id {
			out = append(out, record)
		}
	}
	return out
}

// AssertSent fails unless exactly one notification of type T was delivered to
// this recipient, and returns it.
//
// A method with its own type parameter, which this Go allows on a concrete
// type — so it reads as what it is:
//
//	followed := recorder.AssertSent[Followed](t, ada)
//
// rather than threading the recorder through a package-level function. It is
// the shape cache uses for GetAs and RememberAs, and the reason Recorder is a
// concrete type rather than an interface: a generic method cannot exist on
// one.
//
// The count is exact rather than at-least-one, because "notified twice" is a
// real bug with a real cause — a call site that fires on a repeated upsert —
// and a looser assertion is how it ships.
func (r *Recorder) AssertSent[T notify.Notification](t testing.TB, to notify.Notifiable) T {
	t.Helper()

	var found []T
	for _, record := range r.For(to) {
		if typed, ok := record.Notification.(T); ok {
			found = append(found, typed)
		}
	}

	var zero T
	kind, id := to.NotifiableKey()
	switch len(found) {
	case 1:
		return found[0]
	case 0:
		t.Errorf("%T was not delivered to %s:%s over %q.\nDelivered: %s",
			zero, kind, id, r.Name(), summarise(r.For(to)))
		return zero
	default:
		t.Errorf("%T was delivered %d times to %s:%s over %q, want once",
			zero, len(found), kind, id, r.Name())
		return found[0]
	}
}

// AssertNotSent fails if any notification of type T was delivered to this
// recipient.
func (r *Recorder) AssertNotSent[T notify.Notification](t testing.TB, to notify.Notifiable) {
	t.Helper()

	for _, record := range r.For(to) {
		if _, ok := record.Notification.(T); ok {
			var zero T
			kind, id := to.NotifiableKey()
			t.Errorf("%T was delivered to %s:%s and should not have been",
				zero, kind, id)
			return
		}
	}
}

func summarise(records []Record) string {
	if len(records) == 0 {
		return "nothing"
	}
	out := ""
	for i, record := range records {
		if i > 0 {
			out += ", "
		}
		out += record.Type
	}
	return out
}
