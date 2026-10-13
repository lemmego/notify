package broadcast_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/lemmego/notify"
	bcast "github.com/lemmego/notify/channel/broadcast"
)

type user struct{ id string }

func (u user) NotifiableKey() (string, string) { return "user", u.id }

type ownChannel struct{ user }

func (ownChannel) BroadcastChannel() string { return "private-team.9" }

type created struct{ ID int }

func (created) Via(notify.Notifiable) []string { return []string{bcast.Name} }
func (created) BroadcastEvent() string         { return "notification.created" }

func (c created) ToBroadcast(context.Context, notify.Notifiable) (any, error) {
	return map[string]int{"id": c.ID}, nil
}

type unbroadcastable struct{}

func (unbroadcastable) Via(notify.Notifiable) []string { return []string{bcast.Name} }

type recordingPublisher struct {
	channel string
	event   string
	data    json.RawMessage
	calls   int
	err     error
}

func (p *recordingPublisher) Publish(_ context.Context, channel, event string, data json.RawMessage) error {
	p.calls++
	if p.err != nil {
		return p.err
	}
	p.channel, p.event, p.data = channel, event, data
	return nil
}

func TestANotificationIsPublishedToThePrivateUserChannel(t *testing.T) {
	publisher := &recordingPublisher{}
	n := notify.New(bcast.New(publisher))

	if err := n.Send(context.Background(), user{"42"}, created{ID: 7}); err != nil {
		t.Fatalf("sending: %v", err)
	}

	// The "private-" prefix is part of the name, because the name is what
	// crosses the wire and reaches an authorizer.
	if publisher.channel != "private-user.42" {
		t.Fatalf("published to %q, want private-user.42", publisher.channel)
	}
	if publisher.event != "notification.created" {
		t.Fatalf("got event %q", publisher.event)
	}
	if string(publisher.data) != `{"id":7}` {
		t.Fatalf("got payload %s, want {\"id\":7}", publisher.data)
	}
}

func TestARecipientMayNameItsOwnChannel(t *testing.T) {
	publisher := &recordingPublisher{}
	n := notify.New(bcast.New(publisher))

	if err := n.Send(context.Background(), ownChannel{user{"42"}}, created{ID: 1}); err != nil {
		t.Fatalf("sending: %v", err)
	}
	if publisher.channel != "private-team.9" {
		t.Fatalf("published to %q, want the channel the recipient named", publisher.channel)
	}
}

func TestANotificationThatCannotBeBroadcastSaysWhatIsMissing(t *testing.T) {
	publisher := &recordingPublisher{}
	n := notify.New(bcast.New(publisher))

	err := n.Send(context.Background(), user{"1"}, unbroadcastable{})
	if !errors.Is(err, notify.ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported", err)
	}
	if !strings.Contains(err.Error(), "ToBroadcast") {
		t.Fatalf("the error does not say what is missing: %v", err)
	}
	if publisher.calls != 0 {
		t.Fatal("published anyway")
	}
}

// A failed publish is reported but is not fatal to the notification: the row
// is already written, so it will be seen on the next page load. That is the
// reason database is listed before broadcast.
func TestAFailedPublishIsReported(t *testing.T) {
	publisher := &recordingPublisher{err: errors.New("the bus is unavailable")}
	n := notify.New(bcast.New(publisher))

	err := n.Send(context.Background(), user{"1"}, created{ID: 1})
	if err == nil || !strings.Contains(err.Error(), "the bus is unavailable") {
		t.Fatalf("got %v, want the publisher's own failure", err)
	}
}

func TestAnEmptyEventNameIsRefused(t *testing.T) {
	publisher := &recordingPublisher{}
	n := notify.New(bcast.New(publisher))

	if err := n.Send(context.Background(), user{"1"}, eventless{}); !errors.Is(err, notify.ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported", err)
	}
	if publisher.calls != 0 {
		t.Fatal("published an event with no name for the client to dispatch on")
	}
}

type eventless struct{ created }

func (eventless) BroadcastEvent() string { return "" }

func TestTheChannelNameCanBeReplacedWholesale(t *testing.T) {
	publisher := &recordingPublisher{}
	n := notify.New(bcast.New(publisher, bcast.WithChannelFunc(
		func(to notify.Notifiable) string {
			_, id := to.NotifiableKey()
			return "legacy:" + id
		})))

	if err := n.Send(context.Background(), user{"42"}, created{ID: 1}); err != nil {
		t.Fatalf("sending: %v", err)
	}
	if publisher.channel != "legacy:42" {
		t.Fatalf("published to %q, want legacy:42", publisher.channel)
	}
}

// The publisher is a function as often as it is a type.
func TestAFunctionIsAPublisher(t *testing.T) {
	var seen string
	n := notify.New(bcast.New(bcast.PublishFunc(
		func(_ context.Context, channel, _ string, _ json.RawMessage) error {
			seen = channel
			return nil
		})))

	if err := n.Send(context.Background(), user{"5"}, created{ID: 1}); err != nil {
		t.Fatalf("sending: %v", err)
	}
	if seen != "private-user.5" {
		t.Fatalf("got %q", seen)
	}
}
