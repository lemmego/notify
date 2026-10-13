package notify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
)

// Notifier holds the wired channels and delivers notifications over them.
type Notifier struct {
	channels map[string]Channel
	sink     func(Event)
}

// Option configures a Notifier.
type Option func(*Notifier)

// WithEventSink sends this notifier's events somewhere — typically the
// application's event emitter, which Provider wires when asked.
//
// The sink is called synchronously and a panic in it is contained, because an
// observer must not be able to fail the thing it is observing.
func WithEventSink(sink func(Event)) Option {
	return func(n *Notifier) { n.sink = sink }
}

// New builds a notifier over the given channels.
//
// Two channels claiming one name is a programming error and panics at
// construction: it happens at boot, it is never a runtime condition, and the
// alternative is one of the two silently never being reached.
func New(channels ...Channel) *Notifier {
	return NewWithOptions(channels, nil)
}

// NewWithOptions is New with options.
func NewWithOptions(channels []Channel, opts []Option) *Notifier {
	n := &Notifier{channels: make(map[string]Channel, len(channels))}
	for _, channel := range channels {
		if channel == nil {
			panic("notify: a nil channel was passed to New")
		}
		name := channel.Name()
		if name == "" {
			panic(fmt.Sprintf("notify: channel %T has no name", channel))
		}
		if existing, ok := n.channels[name]; ok {
			panic(fmt.Sprintf("notify: two channels claim the name %q: %T and %T",
				name, existing, channel))
		}
		n.channels[name] = channel
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// Channel returns a wired channel by name.
func (n *Notifier) Channel(name string) (Channel, bool) {
	channel, ok := n.channels[name]
	return channel, ok
}

// Channels names every wired channel, sorted, for an error message or a
// diagnostic.
func (n *Notifier) Channels() []string {
	names := make([]string, 0, len(n.channels))
	for name := range n.channels {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Send delivers a notification to one recipient.
//
// Channels are delivered in the order Via named them, sequentially, on the
// caller's goroutine — and the order is load-bearing rather than incidental: a
// notification listing database before broadcast has its row committed before
// anything tells a browser to go and fetch it. Fanning the channels out across
// goroutines would make that ordering unavailable, to save a few milliseconds
// on operations that are all bounded by construction.
//
// A channel's failure does not stop the ones after it. Every error comes back
// joined, so errors.Is finds any of them, and the ones that indicate a bug
// rather than a state are logged as they happen — a caller that discards this
// error, as a handler reasonably will, still leaves a trace.
//
// Call this after the transaction that caused it has committed, never inside
// it. Inside, a broadcast fires before the row is visible, so the client
// refetches and finds nothing; and if the transaction then rolls back,
// somebody has been told about a notification that does not exist. No
// ordering guarantee in here can save a caller from that.
func (n *Notifier) Send(ctx context.Context, to Notifiable, notification Notification) error {
	if to == nil {
		return errors.New("notify: Send needs a recipient")
	}
	if notification == nil {
		return errors.New("notify: Send needs a notification")
	}

	kind, id := to.NotifiableKey()
	name := fmt.Sprintf("%T", notification)

	var errs []error
	for _, channelName := range notification.Via(to) {
		channel, ok := n.channels[channelName]
		if !ok {
			err := n.fail(&ChannelError{
				Channel: channelName, Notification: name,
				OwnerKind: kind, OwnerID: id,
				Err: fmt.Errorf("%w (wired: %s)", ErrNoChannel,
					strings.Join(n.Channels(), ", ")),
			})
			errs = append(errs, err)
			continue
		}

		n.emit(Sending{Channel: channelName, Notification: name, OwnerKind: kind, OwnerID: id})

		if err := channel.Send(ctx, to, notification); err != nil {
			errs = append(errs, n.fail(&ChannelError{
				Channel: channelName, Notification: name,
				OwnerKind: kind, OwnerID: id, Err: err,
			}))
			continue
		}

		n.emit(Sent{Channel: channelName, Notification: name, OwnerKind: kind, OwnerID: id})
	}
	return errors.Join(errs...)
}

// SendTo delivers one notification to many recipients.
//
// A channel that implements BulkChannel gets the whole slice, so a fan-out to
// a circle is one statement rather than one per member. Recipients are
// grouped by the channel set their Via returns, because Via takes the
// recipient and two of them may legitimately differ.
func (n *Notifier) SendTo(ctx context.Context, recipients []Notifiable, notification Notification) error {
	if notification == nil {
		return errors.New("notify: SendTo needs a notification")
	}

	name := fmt.Sprintf("%T", notification)
	byChannel := map[string][]Notifiable{}
	order := []string{}
	for _, to := range recipients {
		if to == nil {
			continue
		}
		for _, channelName := range notification.Via(to) {
			if _, seen := byChannel[channelName]; !seen {
				order = append(order, channelName)
			}
			byChannel[channelName] = append(byChannel[channelName], to)
		}
	}

	var errs []error
	for _, channelName := range order {
		group := byChannel[channelName]
		channel, ok := n.channels[channelName]
		if !ok {
			errs = append(errs, n.fail(&ChannelError{
				Channel: channelName, Notification: name,
				Err: fmt.Errorf("%w (wired: %s)", ErrNoChannel,
					strings.Join(n.Channels(), ", ")),
			}))
			continue
		}

		if bulk, ok := channel.(BulkChannel); ok {
			if err := bulk.SendMany(ctx, group, notification); err != nil {
				errs = append(errs, n.fail(&ChannelError{
					Channel: channelName, Notification: name, Err: err,
				}))
			}
			continue
		}

		for _, to := range group {
			kind, id := to.NotifiableKey()
			if err := channel.Send(ctx, to, notification); err != nil {
				errs = append(errs, n.fail(&ChannelError{
					Channel: channelName, Notification: name,
					OwnerKind: kind, OwnerID: id, Err: err,
				}))
			}
		}
	}
	return errors.Join(errs...)
}

// Supports reports whether a notification could be delivered on every channel
// its Via names for this recipient, and what is missing where it could not.
//
// Nothing is sent. This is what notifytest.Contract runs, and what makes the
// pairing checkable in a test rather than only in production.
func (n *Notifier) Supports(to Notifiable, notification Notification) error {
	if notification == nil {
		return errors.New("notify: Supports needs a notification")
	}
	name := fmt.Sprintf("%T", notification)

	var errs []error
	for _, channelName := range notification.Via(to) {
		channel, ok := n.channels[channelName]
		if !ok {
			errs = append(errs, fmt.Errorf("%s declares channel %q, which is not wired (wired: %s)",
				name, channelName, strings.Join(n.Channels(), ", ")))
			continue
		}
		asker, ok := channel.(Supports)
		if !ok {
			// A channel that cannot be asked is taken at its word. Saying
			// nothing is better than inventing a failure, and the three
			// built-in channels all answer.
			continue
		}
		if ok, missing := asker.Supports(notification); !ok {
			errs = append(errs, fmt.Errorf("%s declares channel %q but %s",
				name, channelName, missing))
		}
	}
	return errors.Join(errs...)
}

// fail logs the errors that mean a bug and returns the error unchanged.
//
// ErrNoRoute is a state — this person has no email address — so it goes to
// debug. The other two are bugs and go to error, because the caller is
// expected to discard this return value: a failed notification must not fail
// the follow that caused it, which means the log is the only trace left.
func (n *Notifier) fail(err *ChannelError) error {
	level := slog.LevelError
	if errors.Is(err, ErrNoRoute) {
		level = slog.LevelDebug
	}
	slog.Log(context.Background(), level, "notify: a channel did not deliver",
		"channel", err.Channel,
		"notification", err.Notification,
		"owner_kind", err.OwnerKind,
		"owner_id", err.OwnerID,
		"error", err.Err)

	n.emit(Failed{
		Channel: err.Channel, Notification: err.Notification,
		OwnerKind: err.OwnerKind, OwnerID: err.OwnerID, Err: err.Err,
	})
	return err
}
