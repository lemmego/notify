// Package broadcast pushes notifications to whoever is currently looking.
//
// It is the fast path and never the record: the database channel owns that,
// which is why a notification lists database first. A broadcast that is
// dropped costs somebody a page refresh; a row that is missing costs them the
// notification.
//
// The publisher is a four-primitive interface this package declares itself —
// no shared types with any realtime module. That is deliberate: the two are
// developed in parallel, and a channel written against another module's Event
// struct breaks every time that struct moves. A six-line adapter in the
// application is cheaper than a coupling, and it means an application can
// publish through anything at all: lemmego/broadcast, Pusher, a Redis channel
// its own frontend reads, or a recorder in a test.
package broadcast

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lemmego/notify"
)

// Name is what a notification's Via must return to select this channel.
const Name = "broadcast"

// Publisher delivers an event to everybody subscribed to a channel.
//
// Primitives only, so neither side needs the other's types. An
// implementation over lemmego/broadcast is:
//
//	func (p hub) Publish(ctx context.Context, channel, event string, data json.RawMessage) error {
//	    return p.hub.Publish(ctx, broadcast.Channel(channel), broadcast.Event{Name: event, Data: data})
//	}
//
// It should return promptly and must not block on any client: a page render
// is waiting on it. An error means the event did not go out, which notify
// reports and logs without failing anything else — the row is already
// written, so the notification exists and will be seen on the next load.
type Publisher interface {
	Publish(ctx context.Context, channel, event string, data json.RawMessage) error
}

// PublishFunc adapts a function to Publisher.
type PublishFunc func(ctx context.Context, channel, event string, data json.RawMessage) error

func (f PublishFunc) Publish(ctx context.Context, channel, event string, data json.RawMessage) error {
	return f(ctx, channel, event, data)
}

// Broadcastable is the optional interface a notification implements to be
// pushed.
type Broadcastable interface {
	// BroadcastEvent is the event name the client dispatches on, such as
	// "notification.created".
	//
	// Spelled out rather than derived from the Go type, for the same reason
	// the stored type is: it is an API the frontend switches on, and deriving
	// it from an implementation detail makes a rename a breaking change that
	// only shows up as a client that stopped reacting.
	BroadcastEvent() string

	// ToBroadcast returns the payload, which this channel marshals to JSON.
	//
	// Keep it small, and prefer an identifier over an object: this travels
	// over an open connection to every tab somebody has open, and a client
	// that needs the whole thing can fetch it. A payload that is only an id
	// also cannot leak a field somebody later adds to the stored row.
	ToBroadcast(ctx context.Context, to notify.Notifiable) (any, error)
}

// Recipient is the optional interface a notifiable implements to override
// which channel its notifications are published on.
//
// Implement nothing and the name is "private-<kind>.<id>" — "private-user.42"
// — which is the private-channel convention a realtime module's authorizer
// expects. The "private-" prefix is part of the name rather than a flag
// because the name is what crosses the wire, and a privacy marker that can be
// dropped in transit is not a marker.
type Recipient interface {
	BroadcastChannel() string
}

// Channel publishes notifications to a realtime channel.
type Channel struct {
	publisher Publisher
	channelOf func(notify.Notifiable) string
}

// Option configures a Channel.
type Option func(*Channel)

// WithChannelFunc replaces how a recipient's channel name is derived, for an
// application whose realtime naming predates this package.
func WithChannelFunc(fn func(notify.Notifiable) string) Option {
	return func(c *Channel) { c.channelOf = fn }
}

// New builds the channel over a publisher.
func New(publisher Publisher, opts ...Option) *Channel {
	if publisher == nil {
		panic("notify/broadcast: New needs a publisher")
	}
	c := &Channel{publisher: publisher, channelOf: defaultChannel}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Channel) Name() string { return Name }

// Supports reports whether a notification can be broadcast, and what is
// missing when it cannot.
func (c *Channel) Supports(n notify.Notification) (bool, string) {
	if _, ok := n.(Broadcastable); !ok {
		return false, "does not implement broadcast.Broadcastable " +
			"(missing BroadcastEvent and ToBroadcast)"
	}
	return true, ""
}

// Send publishes one notification.
func (c *Channel) Send(ctx context.Context, to notify.Notifiable, n notify.Notification) error {
	broadcastable, ok := n.(Broadcastable)
	if !ok {
		return fmt.Errorf("%w: %T does not implement broadcast.Broadcastable "+
			"(missing BroadcastEvent and ToBroadcast)", notify.ErrUnsupported, n)
	}

	event := broadcastable.BroadcastEvent()
	if event == "" {
		return fmt.Errorf("%w: %T returned an empty BroadcastEvent, which the "+
			"client has nothing to dispatch on", notify.ErrUnsupported, n)
	}

	channel := c.channelOf(to)
	if channel == "" {
		return fmt.Errorf("%w: no broadcast channel for %T", notify.ErrNoRoute, to)
	}

	payload, err := broadcastable.ToBroadcast(ctx, to)
	if err != nil {
		return fmt.Errorf("building the broadcast payload for %T: %w", n, err)
	}
	// Encoded here, so a payload that cannot be marshalled is reported to the
	// caller who can fix it rather than once per subscriber on whichever
	// goroutine was fanning it out.
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encoding the broadcast payload for %T: %w", n, err)
	}

	return c.publisher.Publish(ctx, channel, event, data)
}

// defaultChannel is "private-<kind>.<id>".
func defaultChannel(to notify.Notifiable) string {
	// The override is consulted first. A recipient that names its own channel
	// has said where its notifications go, and that holds whether or not it
	// also has a usable key — a notifiable that is a channel and nothing else
	// is a legitimate shape.
	if override, ok := to.(Recipient); ok {
		if name := override.BroadcastChannel(); name != "" {
			return name
		}
	}
	kind, id := to.NotifiableKey()
	if kind == "" || id == "" {
		return ""
	}
	return "private-" + kind + "." + id
}
