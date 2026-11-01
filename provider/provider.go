// Package provider wires a notifier into an application.
//
// It is a subpackage rather than part of notify, and that is the whole point:
// importing api/app pulls in api's own dependency closure — a Postgres driver
// among them, through api/db — so a root package that offered the provider
// would charge every consumer for it. lemmego/log's 289 modules were the same
// mistake. The root package is standard library only, and a test holds that
// line; everything that needs the container lives here.
package provider

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/lemmego/api/app"
	"github.com/lemmego/notify"
)

// Publisher is the optional interface a channel implements when it has files
// for `lemmego publish` — a migration, a configuration stub.
//
// Provider collects them from its channels, so an application registers one
// provider however many channels it wires, and the file's content stays in
// the package that owns the schema. The alternative was a migration constant
// in the core describing a table it has never read, which is how a schema and
// the queries against it drift apart.
type Publisher interface {
	Publishables() []*app.Publishable
}

// Provider wires a notifier into an application.
//
// The channels are constructed by the application and handed over, which is
// the shape mail.Provider already has with its Transport — and it is what
// keeps this package from importing a SQL store, an SMTP client or a
// WebSocket library in order to offer them. A channel brings its own
// dependencies into the binary only if the application asked for it.
type Provider struct {
	// Channels are the channels to wire. An empty list is a boot error
	// rather than a notifier that silently delivers nothing.
	Channels []notify.Channel

	// Events bridges this package's typed events onto the application's
	// event emitter, under the names in events.go.
	//
	// Off by default, and not because it is expensive. The emitter swallows
	// a listener's error, runs listeners with no panic isolation of its own,
	// and panics if On is called after shutdown — so it is suitable for
	// observation and nothing else. Turning this on is a statement that the
	// listeners are observers.
	Events bool

	// EventSink receives this notifier's events directly, without going
	// through the application's emitter.
	//
	// It exists for observers that want the typed event rather than the
	// emitter's untyped payload — observe/watch/observenotify is the first —
	// and it composes with Events rather than competing with it: set both,
	// and both are called.
	EventSink notify.Listener

	notifier *notify.Notifier
}

func (p *Provider) Provide(a app.App) error {
	if len(p.Channels) == 0 {
		// Refused rather than defaulted. A notifier with no channels accepts
		// every Send, delivers nothing, and reports no error, because Via
		// naming a channel that is not wired is the one failure this would
		// produce for every notification at once — and it would look like an
		// application that simply never notifies anybody.
		return errors.New("notify: Provider has no channels; wire at least one")
	}

	// Collected rather than set one at a time, because WithEventSink holds a
	// single slot: two calls would leave the second in sole possession, and
	// an application that bridged to the emitter and then attached an
	// observer would lose the bridge without being told.
	var sinks []notify.Listener
	if p.Events {
		sinks = append(sinks, func(e notify.Event) {
			a.Dispatch(e.EventName(), e)
		})
	}
	if p.EventSink != nil {
		sinks = append(sinks, p.EventSink)
	}

	opts := []notify.Option{}
	if len(sinks) > 0 {
		opts = append(opts, notify.WithEventSink(fanOut(sinks)))
	}

	p.notifier = notify.NewWithOptions(p.Channels, opts)
	a.AddService(p.notifier)
	notify.SetGlobal(p.notifier)
	return nil
}

// AddPublishables collects the files this notifier's channels want published.
//
// Walking the channels rather than holding the content here is what lets the
// package that owns a schema own its migration too. The alternative was a
// migration constant in this package describing a table it has never read,
// which is how a schema and the queries against it drift apart.
func (p *Provider) AddPublishables() []*app.Publishable {
	var out []*app.Publishable
	for _, channel := range p.Channels {
		if publisher, ok := channel.(Publisher); ok {
			out = append(out, publisher.Publishables()...)
		}
	}
	return out
}

// Shutdown closes any channel that holds something worth closing.
//
// A channel that does not implement io.Closer is left alone. The errors are
// collected rather than returned on the first failure, so one channel that
// cannot close does not strand the others open.
func (p *Provider) Shutdown(ctx context.Context) error {
	var errs []error
	for _, channel := range p.Channels {
		if closer, ok := channel.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, &notify.ChannelError{Channel: channel.Name(), Err: err})
			}
		}
	}
	return joinErrors(errs)
}

func joinErrors(errs []error) error { return errors.Join(errs...) }

// Get resolves the notifier from the container.
func Get(a app.App) (*notify.Notifier, bool) { return app.Lookup[*notify.Notifier](a) }

// fanOut calls every sink, and keeps going when one of them panics.
//
// The notifier already contains a panic in the sink, so an observer cannot
// fail the delivery it is observing. That protects the call as a whole,
// which is not the same as protecting each member: without this, a panic in
// the first sink would silently cost the second its event. log.Fanout
// records the same lesson about its handlers.
func fanOut(sinks []notify.Listener) notify.Listener {
	return func(e notify.Event) {
		for _, sink := range sinks {
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						slog.Error("notify: an event sink panicked",
							"event", e.EventName(), "panic", recovered)
					}
				}()
				sink(e)
			}()
		}
	}
}
