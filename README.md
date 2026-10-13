# lemmego/notify

One notification, delivered over several channels.

A notification is a type that names the channels it goes out on. Each channel
then asks, by type assertion, whether that notification implements the one
optional interface the channel understands.

```go
type Followed struct {
    ActorID     uint64
    ActorHandle string
}

func (Followed) Via(notify.Notifiable) []string {
    return []string{database.Name, broadcast.Name}
}

// database
func (Followed) NotificationType() string { return "followed" }
func (f Followed) ToDatabase(context.Context, notify.Notifiable) (any, error) {
    return map[string]any{"actor_id": f.ActorID, "handle": f.ActorHandle}, nil
}

// broadcast
func (Followed) BroadcastEvent() string { return "notification.created" }
func (f Followed) ToBroadcast(context.Context, notify.Notifiable) (any, error) {
    return map[string]any{"type": "followed"}, nil
}

// Compile-time, one line, and it catches a method whose signature drifted.
var _ database.Storable     = Followed{}
var _ broadcast.Broadcastable = Followed{}
```

Then:

```go
notify.Send(ctx, user, Followed{ActorID: actor.ID, ActorHandle: actor.Handle})
```

## Send after the transaction commits, never inside it

This is the one rule that cannot be enforced from inside the library. Inside a
transaction, the broadcast fires before the row is visible, so a client that
refetches finds nothing — and if the transaction then rolls back, somebody has
been told about a notification that does not exist.

And fire only on a real state transition. An upsert written
`ON CONFLICT DO NOTHING` discards whether anything changed, so a notification
at that call site re-fires on every double-tap. Return whether a row was
actually inserted and send on that.

## The runtime hole, and how it is closed

Go has no duck typing, so a `Via` that names a channel its notification cannot
satisfy is a **runtime** failure. Three things close it:

1. **Channel names are constants in the channel packages**, so `database.Name`
   is checked by the compiler and a typo is a build error. The core defines no
   channel names at all.
2. **A channel returns a named error**, never a silent skip:
   `*Followed says via "mail" but does not implement mail.Mailable (missing
   ToMail)`.
3. **`notifytest.Contract`** moves the check into your suite. This is the test
   an application owes itself the moment it defines a second notification:

```go
func TestNotificationsSatisfyTheirChannels(t *testing.T) {
    notifytest.Contract(t, notifier, notifytest.User{ID: "1"},
        notifications.All()...)
}
```

`All()` is three lines in your own notifications package, returning a zero
value of every type. A notification missing from it is not checked, which is
why the list and the types live together where a reviewer sees both.

## Channels

| Channel | Package | Needs |
|---|---|---|
| `database` | `notify/channel/database` | a `Store`; stdlib only |
| `database` (SQL) | `notify/channel/database/sqlstore` | an `api/db.Connection` |
| `mail` | `notify/channel/mail` | a `*mail.Mailer` |
| `broadcast` | `notify/channel/broadcast` | a four-primitive `Publisher` |

`database` is the record. `mail` and `broadcast` say it is there — which is
why a notification lists `database` first: the order in `Via` is the order of
delivery.

Write your own by implementing `notify.Channel`, and `notify.Supports` so
`Contract` can check it.

## Wiring

```go
conn, _ := db.Resolve(a)
store, err := sqlstore.New(conn)

&provider.Provider{
    Channels: []notify.Channel{
        database.New(store),
        mail.New(lmail.MustGet(a)),
        broadcast.New(hubAdapter{hub}),
    },
}
```

Channels are constructed by the application and handed over, exactly as
`mail.Provider` takes its `Transport`. That is what keeps this module from
importing a SQL driver, an SMTP client or a WebSocket library in order to
offer them.

`lemmego publish --tags=notify-migrations` writes the migration into your own
migrations directory. The schema is yours to read, edit and roll back;
creating tables at boot would put tables in your database that nothing in your
project mentions.

## Deliberate decisions

**The root package is standard library only**, and a test enforces it. The
first version of that test forbade a list of heavy imports and immediately
caught `api/app` reaching a Postgres driver through `api/db` — which is why
`Provider` lives in `notify/provider`. An application that wants in-app
notifications should compile no SMTP client.

**`Send` is synchronous.** The only slow channel is mail, and `mail.Queue`
already renders on the calling goroutine and enqueues a self-contained
message, so the deferral happens at the boundary that needed it. A
`notify.Queue` would have to serialise an arbitrary notification, which means
a name-to-type registry in both directions — the rot `mail.Queue` was designed
to avoid. A slow channel of your own should enqueue its own self-contained
payload, the way mail does.

**The unread count is a capped query, not a counter column.** It reads at most
`MaxUnread+1` entries from a partial index whose entire size is the number of
unread notifications in the system, so it is O(1) in the worst case forever. A
counter column cannot express `col = col + 1` through the ORM, needs raw SQL at
every write, and drifts the first time anybody prunes or deletes a row by
hand — and the symptom is a badge reading 3 forever that no log shows.

**Do not cache it.** An unread badge is state, not discovery, and it is the
thing the person just clicked.

**The store's methods are transitions, not CRUD.** Marking something read is
one conditional update whose affected row count is the answer, so two taps on
one bell cannot both report a change — and the owner is in every condition, so
one person cannot touch another's notifications even if a handler forgets to
check.

**`jsonb` does not preserve your key order**, because it stores a parsed form
— which is the same property that makes it indexable. Every key and value
survives, numbers included; a payload with duplicate keys is collapsed, so do
not build one.

## Not built

Notification preferences (`Via` takes the recipient so they can live in your
application, which is where the product decisions are) · digests and grouping
(the `dedupe_key` column is sized to double as a collapse key later) ·
`notify.Queue` · routes and an inbox UI (`database.Inbox` gives you the five
methods; the page is yours) · MySQL DDL, refused by name rather than
emitted untested · channel-level retry (`tasker`'s `ShouldRetry` owns that).
