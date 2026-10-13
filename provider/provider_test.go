package provider_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lemmego/api/app"
	"github.com/lemmego/notify"
	"github.com/lemmego/notify/notifytest"
	"github.com/lemmego/notify/provider"
)

func TestProvideRegistersTheNotifier(t *testing.T) {
	engine := app.Configure()
	p := &provider.Provider{Channels: []notify.Channel{notifytest.NewRecorder("database")}}

	if err := p.Provide(engine); err != nil {
		t.Fatalf("providing: %v", err)
	}

	if _, ok := provider.Get(engine); !ok {
		t.Fatal("the notifier is not in the container")
	}
	if notify.Global() == nil {
		t.Fatal("the package-level facade was not installed")
	}
}

// A notifier with no channels accepts every Send, delivers nothing and
// reports no error, which looks exactly like an application that never
// notifies anybody. Refused at boot instead.
func TestProvideRefusesAnEmptyChannelList(t *testing.T) {
	engine := app.Configure()
	p := &provider.Provider{}

	err := p.Provide(engine)
	if err == nil {
		t.Fatal("a provider with no channels was accepted")
	}
	if !strings.Contains(err.Error(), "channel") {
		t.Fatalf("the refusal does not say what is missing: %v", err)
	}
}

func TestPublishablesAreCollectedFromTheChannels(t *testing.T) {
	p := &provider.Provider{Channels: []notify.Channel{
		notifytest.NewRecorder("broadcast"), // publishes nothing
		publishingChannel{},
	}}

	published := p.AddPublishables()
	if len(published) != 1 {
		t.Fatalf("collected %d publishables, want 1", len(published))
	}
	if published[0].Tag != "notify-migrations" {
		t.Fatalf("got tag %q", published[0].Tag)
	}
}

type publishingChannel struct{ *notifytest.Recorder }

func (publishingChannel) Name() string { return "database" }

func (publishingChannel) Send(context.Context, notify.Notifiable, notify.Notification) error {
	return nil
}

func (publishingChannel) Publishables() []*app.Publishable {
	return []*app.Publishable{{
		FilePath: "internal/migrations/notifications.go",
		Content:  []byte("// a migration"),
		Tag:      "notify-migrations",
	}}
}

func TestShutdownClosesEveryChannelThatCanBeClosed(t *testing.T) {
	first := &closableChannel{name: "a"}
	second := &closableChannel{name: "b", err: errors.New("could not close")}
	third := &closableChannel{name: "c"}

	p := &provider.Provider{Channels: []notify.Channel{first, second, third}}
	err := p.Shutdown(context.Background())

	if err == nil {
		t.Fatal("the failing channel's error was swallowed")
	}
	// One channel that cannot close must not strand the others open, which is
	// why the errors are collected rather than returned on the first.
	if !first.closed || !third.closed {
		t.Fatal("a later channel was left open after an earlier one failed")
	}
}

type closableChannel struct {
	name   string
	err    error
	closed bool
}

func (c *closableChannel) Name() string { return c.name }

func (c *closableChannel) Send(context.Context, notify.Notifiable, notify.Notification) error {
	return nil
}

func (c *closableChannel) Close() error {
	c.closed = true
	return c.err
}

// The bridge onto the application's emitter is the one use that emitter is
// shaped for, and it only works because api's Dispatch was fixed to pass a
// single payload through as itself: it previously forwarded the whole
// variadic slice, so this listener would have received []any{notify.Sent{}}
// and the assertion below would have failed — which is what every documented
// cache event listener would have hit too.
func TestEventsReachTheApplicationsEmitter(t *testing.T) {
	engine := app.Configure()
	recorder := notifytest.NewRecorder("database")
	p := &provider.Provider{Channels: []notify.Channel{recorder}, Events: true}

	if err := p.Provide(engine); err != nil {
		t.Fatalf("providing: %v", err)
	}

	var got []string
	engine.On(notify.EventSent, func(payload any) error {
		event, ok := payload.(notify.Sent)
		if !ok {
			t.Errorf("listener received %T, want notify.Sent", payload)
			return nil
		}
		got = append(got, event.Channel+":"+event.OwnerID)
		return nil
	})

	notifier, _ := provider.Get(engine)
	if err := notifier.Send(context.Background(), notifytest.User{ID: "42"}, sent{}); err != nil {
		t.Fatalf("sending: %v", err)
	}

	if len(got) != 1 || got[0] != "database:42" {
		t.Fatalf("got %v, want [database:42]", got)
	}
}

type sent struct{}

func (sent) Via(notify.Notifiable) []string { return []string{"database"} }
