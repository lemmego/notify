package notify_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lemmego/notify"
)

// --- doubles -----------------------------------------------------------

type user struct{ id string }

func (u user) NotifiableKey() (string, string) { return "user", u.id }

// recorder is a channel that records what it was asked to deliver.
type recorder struct {
	name string
	sent []string
	err  error
	// supports, when set, makes this channel answer notify.Supports.
	supports func(notify.Notification) (bool, string)
}

func (r *recorder) Name() string { return r.name }

func (r *recorder) Send(_ context.Context, to notify.Notifiable, n notify.Notification) error {
	if r.err != nil {
		return r.err
	}
	_, id := to.NotifiableKey()
	r.sent = append(r.sent, fmt.Sprintf("%T->%s", n, id))
	return nil
}

func (r *recorder) Supports(n notify.Notification) (bool, string) {
	if r.supports == nil {
		return true, ""
	}
	return r.supports(n)
}

// bulk records whether it was given the whole slice at once.
type bulk struct {
	recorder
	batches [][]string
}

func (b *bulk) SendMany(_ context.Context, to []notify.Notifiable, _ notify.Notification) error {
	batch := make([]string, 0, len(to))
	for _, one := range to {
		_, id := one.NotifiableKey()
		batch = append(batch, id)
	}
	b.batches = append(b.batches, batch)
	return nil
}

type followed struct{ via []string }

func (f followed) Via(notify.Notifiable) []string { return f.via }

// --- tests -------------------------------------------------------------

func TestChannelsAreDeliveredInTheOrderViaNamedThem(t *testing.T) {
	var order []string
	first := &recorder{name: "database"}
	second := &recorder{name: "broadcast"}
	n := notify.New(
		orderTracking{first, &order},
		orderTracking{second, &order},
	)

	// The order is load-bearing: a row has to exist before anything tells a
	// browser to go and look for it.
	if err := n.Send(context.Background(), user{"1"},
		followed{via: []string{"database", "broadcast"}}); err != nil {
		t.Fatalf("sending: %v", err)
	}

	if got := strings.Join(order, ","); got != "database,broadcast" {
		t.Fatalf("delivered %s, want database,broadcast", got)
	}
}

type orderTracking struct {
	inner *recorder
	order *[]string
}

func (o orderTracking) Name() string { return o.inner.Name() }
func (o orderTracking) Send(ctx context.Context, to notify.Notifiable, n notify.Notification) error {
	*o.order = append(*o.order, o.inner.Name())
	return o.inner.Send(ctx, to, n)
}

func TestAFailingChannelDoesNotStopTheOnesAfterIt(t *testing.T) {
	broken := &recorder{name: "mail", err: errors.New("the relay refused it")}
	working := &recorder{name: "database"}
	n := notify.New(broken, working)

	err := n.Send(context.Background(), user{"7"},
		followed{via: []string{"mail", "database"}})

	if err == nil {
		t.Fatal("got no error, want the mail channel's failure reported")
	}
	if len(working.sent) != 1 {
		t.Fatalf("the database channel delivered %d times, want 1 — one dead "+
			"channel must not take the others with it", len(working.sent))
	}
	if !strings.Contains(err.Error(), "the relay refused it") {
		t.Fatalf("the error lost its cause: %v", err)
	}
}

func TestAnUnwiredChannelIsReportedWithWhatIsWired(t *testing.T) {
	n := notify.New(&recorder{name: "database"})

	err := n.Send(context.Background(), user{"1"}, followed{via: []string{"mial"}})

	if !errors.Is(err, notify.ErrNoChannel) {
		t.Fatalf("got %v, want ErrNoChannel through the join and the wrap", err)
	}
	// A typo is the usual cause, so the error has to say what was available.
	if !strings.Contains(err.Error(), "database") {
		t.Fatalf("the error does not name the wired channels: %v", err)
	}
}

func TestNoChannelsIsNotAnError(t *testing.T) {
	channel := &recorder{name: "database"}
	n := notify.New(channel)

	// Somebody who has muted this kind of notification entirely.
	if err := n.Send(context.Background(), user{"1"}, followed{via: nil}); err != nil {
		t.Fatalf("got %v, want nil — an empty Via is an answer, not a failure", err)
	}
	if len(channel.sent) != 0 {
		t.Fatal("delivered despite Via naming no channels")
	}
}

func TestSupportsReportsWhatIsMissingWithoutSending(t *testing.T) {
	channel := &recorder{
		name: "mail",
		supports: func(notify.Notification) (bool, string) {
			return false, "does not implement mail.Mailable (missing ToMail)"
		},
	}
	n := notify.New(channel)

	err := n.Supports(user{"1"}, followed{via: []string{"mail"}})
	if err == nil {
		t.Fatal("got no error, want the missing interface reported")
	}
	if !strings.Contains(err.Error(), "missing ToMail") {
		t.Fatalf("the failure does not say what is missing: %v", err)
	}
	if len(channel.sent) != 0 {
		t.Fatal("Supports delivered something; it must only ask")
	}
}

func TestSendToGivesABulkChannelEverybodyAtOnce(t *testing.T) {
	channel := &bulk{recorder: recorder{name: "database"}}
	n := notify.New(channel)

	recipients := []notify.Notifiable{user{"1"}, user{"2"}, user{"3"}}
	if err := n.SendTo(context.Background(), recipients,
		followed{via: []string{"database"}}); err != nil {
		t.Fatalf("sending: %v", err)
	}

	// One statement for a circle, not one per member.
	if len(channel.batches) != 1 {
		t.Fatalf("got %d batches, want 1", len(channel.batches))
	}
	if got := strings.Join(channel.batches[0], ","); got != "1,2,3" {
		t.Fatalf("got batch %s, want 1,2,3", got)
	}
}

func TestSendToLoopsAChannelThatCannotDoBulk(t *testing.T) {
	channel := &recorder{name: "broadcast"}
	n := notify.New(channel)

	if err := n.SendTo(context.Background(),
		[]notify.Notifiable{user{"1"}, user{"2"}},
		followed{via: []string{"broadcast"}}); err != nil {
		t.Fatalf("sending: %v", err)
	}
	if len(channel.sent) != 2 {
		t.Fatalf("delivered %d times, want 2", len(channel.sent))
	}
}

func TestTwoChannelsClaimingOneNamePanicAtConstruction(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("got no panic — two channels with one name means one of " +
				"them is silently never reached")
		}
		if !strings.Contains(fmt.Sprint(r), "database") {
			t.Fatalf("the panic does not name the collision: %v", r)
		}
	}()

	notify.New(&recorder{name: "database"}, &recorder{name: "database"})
}

func TestEventsAreEmittedForEachChannel(t *testing.T) {
	var names []string
	n := notify.NewWithOptions(
		[]notify.Channel{&recorder{name: "database"}},
		[]notify.Option{notify.WithEventSink(func(e notify.Event) {
			names = append(names, e.EventName())
		})},
	)

	if err := n.Send(context.Background(), user{"1"},
		followed{via: []string{"database"}}); err != nil {
		t.Fatalf("sending: %v", err)
	}

	if got := strings.Join(names, ","); got != "notify.sending,notify.sent" {
		t.Fatalf("got events %s, want notify.sending,notify.sent", got)
	}
}

func TestAPanickingListenerDoesNotFailTheDelivery(t *testing.T) {
	channel := &recorder{name: "database"}
	n := notify.NewWithOptions(
		[]notify.Channel{channel},
		[]notify.Option{notify.WithEventSink(func(notify.Event) {
			panic("a listener that should not be able to do this")
		})},
	)

	if err := n.Send(context.Background(), user{"1"},
		followed{via: []string{"database"}}); err != nil {
		t.Fatalf("got %v, want nil — an observer must not be able to fail the "+
			"thing it is observing", err)
	}
	if len(channel.sent) != 1 {
		t.Fatal("the notification was not delivered")
	}
}

func TestSendWithoutANotifierReportsIt(t *testing.T) {
	notify.SetGlobal(nil)
	err := notify.Send(context.Background(), user{"1"}, followed{via: []string{"database"}})
	if !errors.Is(err, notify.ErrNoNotifier) {
		t.Fatalf("got %v, want ErrNoNotifier — a missing provider must not look "+
			"like an application that never notifies anybody", err)
	}
}
