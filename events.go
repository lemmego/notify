package notify

import (
	"context"
	"log/slog"
)

// Typed events, on the shape cache established.
//
// They are typed structs with a Listener taking the interface, rather than
// going through the application's emitter directly, because that emitter
// swallows a listener's error, panics on On after shutdown, and runs listeners
// with no panic isolation. Provider bridges these onto it when asked, which is
// the one thing the emitter is actually shaped for: observation.

// Event names. These are what a listener registered on the application's
// emitter selects on.
const (
	EventSending = "notify.sending"
	EventSent    = "notify.sent"
	EventFailed  = "notify.failed"
)

// Event is one thing that happened while delivering.
type Event interface{ EventName() string }

// Listener observes events.
type Listener func(Event)

// Sending is emitted before a channel is asked to deliver.
type Sending struct {
	Channel      string
	Notification string
	OwnerKind    string
	OwnerID      string
}

func (Sending) EventName() string { return EventSending }

// Sent is emitted after a channel delivered without error.
type Sent struct {
	Channel      string
	Notification string
	OwnerKind    string
	OwnerID      string
}

func (Sent) EventName() string { return EventSent }

// Failed is emitted when a channel did not deliver.
//
// It carries the unwrapped cause rather than the *ChannelError, because the
// surrounding fields already say which channel and which notification, and a
// listener that wants to branch wants errors.Is on the cause.
type Failed struct {
	Channel      string
	Notification string
	OwnerKind    string
	OwnerID      string
	Err          error
}

func (Failed) EventName() string { return EventFailed }

// emit delivers one event to the sink, if there is one.
//
// A panicking sink is contained and reported, never propagated: an observer
// must not be able to fail the delivery it is observing. cache learned the
// same thing and added its own recover for it.
func (n *Notifier) emit(e Event) {
	if n.sink == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			reportSinkPanic(e, r)
		}
	}()
	n.sink(e)
}

func reportSinkPanic(e Event, r any) {
	slog.Log(context.Background(), slog.LevelError,
		"notify: an event listener panicked and was contained",
		"event", e.EventName(), "panic", r)
}
