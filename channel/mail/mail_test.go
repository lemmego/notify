package mail_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	lmail "github.com/lemmego/mail"
	"github.com/lemmego/mail/mailtest"

	"github.com/lemmego/notify"
	mailchan "github.com/lemmego/notify/channel/mail"
)

type user struct {
	id    string
	email string
}

func (u user) NotifiableKey() (string, string) { return "user", u.id }

func (u user) MailAddress() (lmail.Address, bool) {
	if u.email == "" {
		return lmail.Address{}, false
	}
	return lmail.Address{Name: "Ada", Email: u.email}, true
}

// unroutable has no MailAddress at all, which is the shape of a notifiable
// that was never meant to be mailed.
type unroutable struct{}

func (unroutable) NotifiableKey() (string, string) { return "system", "1" }

type matched struct{ to []lmail.Address }

func (matched) Via(notify.Notifiable) []string { return []string{mailchan.Name} }

func (m matched) ToMail(context.Context, notify.Notifiable) (lmail.Mailable, error) {
	return body{to: m.to}, nil
}

type body struct{ to []lmail.Address }

func (b body) Build(lmail.Context, lmail.Renderer) (*lmail.Message, error) {
	return &lmail.Message{
		To:      b.to,
		Subject: "You matched",
		Text:    "Somebody wants to talk to you.",
	}, nil
}

// unmailable names the mail channel without implementing Mailable.
type unmailable struct{}

func (unmailable) Via(notify.Notifiable) []string { return []string{mailchan.Name} }

func mailer(t *testing.T) (*lmail.Mailer, *mailtest.Recorder) {
	t.Helper()
	recorder := mailtest.NewRecorder()
	return lmail.New(recorder, lmail.NewRenderer(t.TempDir()),
		lmail.Address{Name: "Productshore", Email: "hello@example.com"}), recorder
}

func TestANotificationIsMailedToTheRecipient(t *testing.T) {
	sender, recorder := mailer(t)
	// Inline rather than queued, because a test asserting on the transport
	// should not also need a worker.
	n := notify.New(mailchan.New(sender, mailchan.WithQueue(false)))

	if err := n.Send(context.Background(), user{"1", "ada@example.com"}, matched{}); err != nil {
		t.Fatalf("sending: %v", err)
	}

	if recorder.Count() != 1 {
		t.Fatalf("sent %d messages, want 1", recorder.Count())
	}
	message := recorder.Last()
	if len(message.To) != 1 || message.To[0].Email != "ada@example.com" {
		t.Fatalf("addressed to %v, want ada@example.com", message.To)
	}
	if message.Subject != "You matched" {
		t.Fatalf("got subject %q", message.Subject)
	}
	// Filled in from the mailer's own From, since the notification set none.
	if message.From.Email != "hello@example.com" {
		t.Fatalf("got from %q, want the mailer's default", message.From.Email)
	}
}

func TestANotificationMayAddressItsOwnMail(t *testing.T) {
	sender, recorder := mailer(t)
	n := notify.New(mailchan.New(sender, mailchan.WithQueue(false)))

	// A notification addressed somewhere other than its notifiable — an
	// operator, a shared inbox — must not be overwritten.
	operator := []lmail.Address{{Email: "ops@example.com"}}
	if err := n.Send(context.Background(), user{"1", "ada@example.com"},
		matched{to: operator}); err != nil {
		t.Fatalf("sending: %v", err)
	}

	if got := recorder.Last().To[0].Email; got != "ops@example.com" {
		t.Fatalf("addressed to %q, want the address the notification set", got)
	}
}

func TestARecipientWithNoAddressIsReportedAsNoRoute(t *testing.T) {
	sender, recorder := mailer(t)
	n := notify.New(mailchan.New(sender, mailchan.WithQueue(false)))

	err := n.Send(context.Background(), user{"1", ""}, matched{})
	if !errors.Is(err, notify.ErrNoRoute) {
		t.Fatalf("got %v, want ErrNoRoute — an unconfirmed address is a state, "+
			"not a bug, and notify logs the two differently", err)
	}
	if recorder.Count() != 0 {
		t.Fatal("a message was sent to nobody")
	}
}

func TestANotifiableThatCannotBeMailedSaysWhatIsMissing(t *testing.T) {
	sender, _ := mailer(t)
	n := notify.New(mailchan.New(sender, mailchan.WithQueue(false)))

	err := n.Send(context.Background(), unroutable{}, matched{})
	if !errors.Is(err, notify.ErrNoRoute) {
		t.Fatalf("got %v, want ErrNoRoute", err)
	}
	if !strings.Contains(err.Error(), "MailAddress") {
		t.Fatalf("the error does not name the missing method: %v", err)
	}
}

func TestANotificationThatCannotBeMailedSaysWhatIsMissing(t *testing.T) {
	sender, _ := mailer(t)
	channel := mailchan.New(sender, mailchan.WithQueue(false))
	n := notify.New(channel)

	err := n.Send(context.Background(), user{"1", "ada@example.com"}, unmailable{})
	if !errors.Is(err, notify.ErrUnsupported) {
		t.Fatalf("got %v, want ErrUnsupported", err)
	}
	if !strings.Contains(err.Error(), "mail.Mailable") || !strings.Contains(err.Error(), "ToMail") {
		t.Fatalf("the error does not say what is missing: %v", err)
	}

	// And Supports says the same sentence without sending anything, which is
	// what makes the pairing checkable in a test.
	if err := n.Supports(user{"1", "ada@example.com"}, unmailable{}); err == nil ||
		!strings.Contains(err.Error(), "ToMail") {
		t.Fatalf("Supports did not report the missing method: %v", err)
	}
}

func TestATransportFailureIsReported(t *testing.T) {
	recorder := mailtest.NewRecorder()
	recorder.Err = errors.New("the relay refused it")
	sender := lmail.New(recorder, lmail.NewRenderer(t.TempDir()),
		lmail.Address{Email: "hello@example.com"})
	n := notify.New(mailchan.New(sender, mailchan.WithQueue(false)))

	err := n.Send(context.Background(), user{"1", "ada@example.com"}, matched{})
	if err == nil || !strings.Contains(err.Error(), "the relay refused it") {
		t.Fatalf("got %v, want the transport's own failure", err)
	}
}
