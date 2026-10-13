// Package notify delivers one notification over several channels.
//
// A notification is a type with a Via method naming the channels it should go
// out on. Each channel then asks, by type assertion, whether that notification
// implements the one optional interface the channel understands — ToDatabase,
// ToMail, ToBroadcast. That is as close as Go gets to Laravel's duck typing,
// and the price is that a Via naming a channel its notification cannot satisfy
// is a runtime failure rather than a compile-time one.
//
// The price is paid once, in a test. See notifytest.Contract, and put
//
//	var _ mail.Mailable = FollowedYou{}
//
// beside each notification type: one line, compile-time, and it catches the
// common case of a method whose signature drifted.
//
// This package deliberately knows nothing about email addresses, SQL or
// WebSockets. Every channel owns its own optional interfaces and its own
// dependencies, so an application that only wants in-app notifications
// compiles no SMTP client. deps_test.go holds that line rather than trusting
// anybody to remember it.
package notify

import (
	"context"
	"errors"
	"fmt"
)

// Notification is one thing worth telling somebody about.
type Notification interface {
	// Via names the channels this notification goes out on for this
	// recipient.
	//
	// Returning nil is a legitimate answer rather than an error: somebody who
	// has muted this kind of notification entirely gets no channels. The
	// order is honoured — channels are delivered in the order named, which is
	// how a notification makes sure its row exists before anything tells a
	// browser to go and look for it.
	Via(to Notifiable) []string
}

// Notifiable is somebody or something that can receive a notification.
//
// One method, returning the pair that identifies the recipient: a type
// discriminator and an id. That is deliberately the whole contract.
// Everything else a channel needs — an email address, a broadcast channel
// name — is a separate optional interface declared beside that channel, so
// this package never learns what an email address is.
//
// Both halves are strings although an application's users are very likely
// keyed on an integer, because auth's own seam is a string and because a
// project may key them on a uuid instead.
type Notifiable interface {
	NotifiableKey() (kind, id string)
}

// Channel delivers notifications over one medium.
//
// An application supplies its own by implementing this and passing it to New.
// There is nothing else to register.
type Channel interface {
	// Name is what a notification's Via must return to select this channel.
	//
	// Each built-in channel exports this as a constant, so a notification
	// writes []string{database.Name, mail.Name} and a typo is a compile
	// error. This package defines no channel names of its own.
	Name() string

	// Send delivers one notification to one recipient, on the caller's
	// goroutine.
	//
	// A notification this channel cannot handle must come back wrapping
	// ErrUnsupported, and a recipient it cannot route to must wrap ErrNoRoute.
	// Neither may be absorbed quietly: the first is a bug, and the second is
	// usually one.
	Send(ctx context.Context, to Notifiable, n Notification) error
}

// BulkChannel is the optional interface a channel implements when it can
// deliver to many recipients in one go.
//
// A notification fanned out to two hundred circle members should be one
// multi-row insert, not two hundred. A channel that does not implement this
// is looped by SendTo, which is correct but slower.
type BulkChannel interface {
	SendMany(ctx context.Context, to []Notifiable, n Notification) error
}

// Supports is the optional interface a channel implements to answer, without
// sending anything, whether it could deliver a notification — and when it
// could not, what is missing.
//
// It exists so notifytest.Contract can check every pairing an application
// defines without a mail relay or a database, and so the runtime error and
// the test failure are the same sentence. "unsupported" on its own sends
// whoever reads it into the channel's source to work out which interface they
// owed; "does not implement mail.Mailable (missing ToMail)" does not.
type Supports interface {
	Supports(n Notification) (ok bool, missing string)
}

var (
	// ErrNoChannel means Via named a channel that is not wired. The error
	// lists what is wired, because the cause is nearly always a typo or a
	// provider that was never given the channel.
	ErrNoChannel = errors.New("notify: no such channel")

	// ErrUnsupported means the channel is wired but this notification does
	// not implement the optional interface it needs.
	ErrUnsupported = errors.New("notify: notification does not support this channel")

	// ErrNoRoute means the recipient has no address on this channel.
	ErrNoRoute = errors.New("notify: recipient has no route on this channel")
)

// ChannelError names which channel failed, for which notification and for
// whom, so one line in a log is enough to find the bug.
type ChannelError struct {
	Channel      string
	Notification string // the notification's Go type
	OwnerKind    string
	OwnerID      string
	Err          error
}

func (e *ChannelError) Error() string {
	return fmt.Sprintf("notify: %s over %q to %s:%s: %v",
		e.Notification, e.Channel, e.OwnerKind, e.OwnerID, e.Err)
}

func (e *ChannelError) Unwrap() error { return e.Err }
