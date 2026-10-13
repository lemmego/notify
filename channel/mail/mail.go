// Package mail sends notifications by email, over lemmego/mail.
//
// It is a thin channel on purpose. mail.Queue already renders the body on the
// calling goroutine and enqueues a self-contained *mail.Message, so the one
// slow part of sending email — the SMTP conversation — is already off the
// request path without this package arranging anything. That is also why
// notify.Send is synchronous: the deferral happens at the boundary that
// needed it.
package mail

import (
	"context"
	"fmt"

	lmail "github.com/lemmego/mail"
	"github.com/lemmego/tasker"

	"github.com/lemmego/notify"
)

// Name is what a notification's Via must return to select this channel.
const Name = "mail"

// Mailable is the optional interface a notification implements to go by mail.
//
// It returns a mail.Mailable rather than a rendered *mail.Message, so the
// body is rendered inside the mailer with the application's own renderer and
// a notification's email is written exactly like every other mailable in the
// project.
//
// Leave the Message's To empty and this channel fills it in from the
// Recipient. Set it and yours wins: a notification addressed to an operator
// rather than to its own notifiable is a real case and should not need a
// second channel to express.
type Mailable interface {
	ToMail(ctx context.Context, to notify.Notifiable) (lmail.Mailable, error)
}

// Recipient is the optional interface a notifiable implements to say where
// its mail goes.
//
// The bool rather than an error, because "this person has not confirmed an
// address" is a routing decision and not a failure, and because a notifiable
// that is already loaded does no I/O to answer. A notifiable implementing
// nothing here cannot be mailed, and the channel says so with ErrNoRoute
// rather than returning quietly — a notification that silently went nowhere
// is the hardest kind to be told about.
type Recipient interface {
	MailAddress() (lmail.Address, bool)
}

// Channel sends notifications by email.
type Channel struct {
	mailer      *lmail.Mailer
	queue       bool
	dispatchOpt []tasker.DispatchOpt
}

// Option configures a Channel.
type Option func(*Channel)

// WithQueue chooses between queueing and sending inline.
//
// Queueing is the default and sending inline is almost always wrong: it puts
// a conversation with somebody else's mail server on the request path, where
// a slow relay becomes a slow page and a dead one becomes a failed follow.
// It exists for a test, and for a command-line process where there is no
// worker to pick the job up.
func WithQueue(queue bool) Option {
	return func(c *Channel) { c.queue = queue }
}

// WithDispatchOpts passes options through to the queued send — a named queue,
// a delay.
func WithDispatchOpts(opts ...tasker.DispatchOpt) Option {
	return func(c *Channel) { c.dispatchOpt = opts }
}

// New builds the channel over a mailer.
func New(mailer *lmail.Mailer, opts ...Option) *Channel {
	if mailer == nil {
		panic("notify/mail: New needs a mailer")
	}
	c := &Channel{mailer: mailer, queue: true}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Channel) Name() string { return Name }

// Supports reports whether a notification can be mailed, and what is missing
// when it cannot.
func (c *Channel) Supports(n notify.Notification) (bool, string) {
	if _, ok := n.(Mailable); !ok {
		return false, "does not implement mail.Mailable (missing ToMail)"
	}
	return true, ""
}

// Send mails one notification.
func (c *Channel) Send(ctx context.Context, to notify.Notifiable, n notify.Notification) error {
	mailable, ok := n.(Mailable)
	if !ok {
		return fmt.Errorf("%w: %T does not implement mail.Mailable (missing ToMail)",
			notify.ErrUnsupported, n)
	}

	recipient, ok := to.(Recipient)
	if !ok {
		return fmt.Errorf("%w: %T does not implement mail.Recipient (missing MailAddress)",
			notify.ErrNoRoute, to)
	}
	address, ok := recipient.MailAddress()
	if !ok || address.Email == "" {
		// A state rather than a bug — an unconfirmed address, usually — which
		// is why notify logs ErrNoRoute at debug and the others at error.
		return fmt.Errorf("%w: %T has no email address", notify.ErrNoRoute, to)
	}

	built, err := mailable.ToMail(ctx, to)
	if err != nil {
		return fmt.Errorf("building the mail for %T: %w", n, err)
	}
	if built == nil {
		return fmt.Errorf("%w: %T returned a nil mailable", notify.ErrUnsupported, n)
	}

	addressed := addressedTo{inner: built, to: []lmail.Address{address}}
	if c.queue {
		return c.mailer.Queue(ctx, addressed, c.dispatchOpt...)
	}
	return c.mailer.Send(ctx, addressed)
}

// addressedTo fills in a recipient the notification did not name.
//
// The mailer builds the mailable itself, so this channel cannot reach the
// Message in order to address it — it wraps the mailable and addresses the
// Message on its way out. Doing it after Build rather than before is what
// lets a notification override the address by setting one, and it lands
// before the mailer validates, which rejects a message with no recipient.
type addressedTo struct {
	inner lmail.Mailable
	to    []lmail.Address
}

func (a addressedTo) Build(ctx lmail.Context, r lmail.Renderer) (*lmail.Message, error) {
	message, err := a.inner.Build(ctx, r)
	if err != nil {
		return nil, err
	}
	if message == nil {
		return nil, fmt.Errorf("notify/mail: %T built a nil message", a.inner)
	}
	if len(message.To) == 0 {
		message.To = a.to
	}
	return message, nil
}
