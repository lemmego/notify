package provider_test

import (
	"context"
	"testing"

	"github.com/lemmego/api/app"
	"github.com/lemmego/notify"
	"github.com/lemmego/notify/notifytest"
	"github.com/lemmego/notify/provider"
)

// recipient is the minimum a Notifiable has to be.
type recipient struct{ id string }

func (r recipient) NotifiableKey() (string, string) { return "user", r.id }

// TestBothSinksAreCalled is the regression for the single slot.
//
// WithEventSink assigns rather than appends, so wiring the emitter bridge and
// an observer as two separate calls leaves one of them in sole possession.
// Which one depends on the order they were applied in, and the loser goes
// quiet without saying so.
//
// Asserting the EventSink alone would not catch that: the old behaviour
// applied the sinks in turn, so the last one — the EventSink — survived and
// an observer-only assertion passed against the bug. The emitter bridge is
// the half that was lost, so it is the half worth asserting.
func TestBothSinksAreCalled(t *testing.T) {
	var viaSink, viaEmitter int

	engine := app.Configure()
	engine.On(notify.EventSent, func(context.Context, any) error { viaEmitter++; return nil })

	p := &provider.Provider{
		Channels:  []notify.Channel{notifytest.NewRecorder("database")},
		Events:    true,
		EventSink: func(context.Context, notify.Event) { viaSink++ },
	}
	if err := p.Provide(engine); err != nil {
		t.Fatalf("providing: %v", err)
	}
	sendOne(t, engine)

	if viaEmitter == 0 {
		t.Error("the emitter bridge received nothing; it lost its slot to the EventSink")
	}
	if viaSink == 0 {
		t.Error("the EventSink received nothing; it lost its slot to the emitter bridge")
	}
}

// TestASinkThatPanicsDoesNotCostTheOtherOneItsEvent.
//
// The notifier contains a panic in the sink, so an observer cannot fail the
// delivery it observes. That protects the call as a whole, which is not the
// same as protecting each member: without the per-sink recover a panic in
// the first sink escapes the fan-out, the notifier absorbs it there, and the
// second sink silently never runs.
//
// The panic comes from a listener on the application's emitter, which is a
// real way to get one — that emitter runs listeners with no recover of its
// own — and it is the first sink, because the bridge is added before the
// EventSink.
//
// Both halves are asserted. The bridge having run is what distinguishes this
// from the single-slot behaviour, where the bridge was never installed and
// so nothing panicked at all.
func TestASinkThatPanicsDoesNotCostTheOtherOneItsEvent(t *testing.T) {
	var bridgeRan, sinkRan bool

	engine := app.Configure()
	engine.On(notify.EventSent, func(context.Context, any) error {
		bridgeRan = true
		panic("a listener that panics")
	})

	p := &provider.Provider{
		Channels: []notify.Channel{notifytest.NewRecorder("database")},
		Events:   true,
		// Only the event that panics counts. notify emits Sending before
		// Sent, so a sink recording any event at all has already run by the
		// time the panicking one arrives — which made the first version of
		// this test pass with the per-sink recover deleted.
		EventSink: func(_ context.Context, e notify.Event) {
			if e.EventName() == notify.EventSent {
				sinkRan = true
			}
		},
	}
	if err := p.Provide(engine); err != nil {
		t.Fatalf("providing: %v", err)
	}
	sendOne(t, engine)

	if !bridgeRan {
		t.Fatal("the emitter bridge never ran, so nothing panicked and this proves nothing")
	}
	if !sinkRan {
		t.Error("a panic in the first sink stopped the second from being called")
	}
}

func sendOne(t *testing.T, engine app.App) {
	t.Helper()
	notifier, ok := provider.Get(engine)
	if !ok {
		t.Fatal("the notifier is not in the container")
	}
	if err := notifier.Send(context.Background(), recipient{id: "7"}, sent{}); err != nil {
		t.Fatalf("sending: %v", err)
	}
}
