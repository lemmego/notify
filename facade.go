package notify

import (
	"context"
	"errors"
)

// ErrNoNotifier reports that nothing has been registered yet, which in
// practice means notify.Provider is missing from the provider list.
var ErrNoNotifier = errors.New("notify: no notifier is installed")

var global *Notifier

// SetGlobal installs the notifier the package-level functions use. Provider
// calls it; an application normally should not.
func SetGlobal(n *Notifier) { global = n }

// Global returns the installed notifier, or nil.
func Global() *Notifier { return global }

// Send delivers a notification through the installed notifier.
//
// Call it after the transaction that caused the notification has committed,
// never inside it: a broadcast fired inside one arrives before the row is
// visible, so a client that refetches finds nothing — and if the transaction
// then rolls back, somebody has been told about something that does not
// exist.
func Send(ctx context.Context, to Notifiable, n Notification) error {
	if global == nil {
		return ErrNoNotifier
	}
	return global.Send(ctx, to, n)
}

// SendTo delivers one notification to many recipients.
func SendTo(ctx context.Context, recipients []Notifiable, n Notification) error {
	if global == nil {
		return ErrNoNotifier
	}
	return global.SendTo(ctx, recipients, n)
}
