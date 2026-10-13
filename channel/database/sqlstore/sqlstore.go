package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lemmego/api/db"
	"github.com/lemmego/notify/channel/database"
)

// Store keeps notifications in a SQL database.
type Store struct {
	pool    *sql.DB
	dialect db.Dialect
	table   string
}

var _ database.Store = (*Store)(nil)

// Option configures a Store.
type Option func(*Store)

// WithTable moves the table, for a project whose schema is namespaced.
func WithTable(name string) Option {
	return func(s *Store) { s.table = name }
}

// New builds a store over the application's connection.
//
// The connection is borrowed: the connector that registered it owns the pool
// and closes it at shutdown, so this store never does.
//
// A dialect this package has no statements for is refused by name rather than
// accepted and then failing on the first query. Being told at boot which two
// dialects exist is worth more than discovering it when somebody follows you.
func New(conn db.Connection, opts ...Option) (*Store, error) {
	if conn == nil {
		return nil, fmt.Errorf("notify/sqlstore: New needs a connection; " +
			"is a database connector registered before notify in LoadProviders?")
	}
	s := &Store{pool: conn.SQLDB(), dialect: conn.Dialect(), table: DefaultTable}
	for _, opt := range opts {
		opt(s)
	}
	if SchemaStatements(s.dialect, s.table) == nil {
		return nil, fmt.Errorf("notify/sqlstore: no statements for dialect %q "+
			"(this package has postgres and sqlite)", s.dialect)
	}
	return s, nil
}

// NewWithPool is New for a caller holding a pool and a dialect directly,
// which is what a test fixture has before an application exists.
func NewWithPool(pool *sql.DB, dialect db.Dialect, opts ...Option) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("notify/sqlstore: NewWithPool needs a pool")
	}
	s := &Store{pool: pool, dialect: dialect, table: DefaultTable}
	for _, opt := range opts {
		opt(s)
	}
	if SchemaStatements(s.dialect, s.table) == nil {
		return nil, fmt.Errorf("notify/sqlstore: no statements for dialect %q "+
			"(this package has postgres and sqlite)", s.dialect)
	}
	return s, nil
}

// Migrate creates the table and its indexes if they are absent.
//
// Published migrations are the supported route — a schema is the
// application's to read, edit and roll back — so this exists for a test
// fixture and for a project that has decided it would rather not have one.
func (s *Store) Migrate(ctx context.Context) error {
	for _, statement := range SchemaStatements(s.dialect, s.table) {
		if _, err := s.pool.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("notify/sqlstore: %w", err)
		}
	}
	return nil
}

// ph renders placeholder n, one-based.
//
// Postgres numbers them and the others do not, which is the same split
// orm's dialects draw. Getting it wrong is not subtle — the statement fails
// to prepare — so this is about keeping one spelling in one place.
func (s *Store) ph(n int) string {
	if s.dialect == db.Postgres {
		return "$" + strconv.Itoa(n)
	}
	return "?"
}

func (s *Store) Insert(ctx context.Context, rows []database.Row) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// One statement for the whole slice, which is what makes a fan-out to a
	// circle one insert rather than one per member.
	var (
		values = make([]string, 0, len(rows))
		args   = make([]any, 0, len(rows)*6)
		n      = 1
	)
	for _, row := range rows {
		values = append(values, fmt.Sprintf("(%s, %s, %s, %s, %s, %s)",
			s.ph(n), s.ph(n+1), s.ph(n+2), s.ph(n+3), s.ph(n+4), s.ph(n+5)))
		n += 6

		data := row.Data
		if len(data) == 0 {
			data = json.RawMessage("{}")
		}
		// A dedupe key is NULL rather than "" when absent, because the unique
		// index is partial on IS NOT NULL — an empty string would collide
		// with every other keyless row for the same owner.
		var key any
		if row.DedupeKey != "" {
			key = row.DedupeKey
		}
		created := row.CreatedAt
		if created.IsZero() {
			created = time.Now()
		}
		args = append(args, row.Owner.Kind, row.Owner.ID, row.Type,
			string(data), key, s.writeTime(created.UTC()))
	}

	statement := fmt.Sprintf(
		`INSERT INTO %s (notifiable_type, notifiable_id, type, data, dedupe_key, created_at)
VALUES %s
ON CONFLICT (notifiable_type, notifiable_id, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING`,
		s.table, strings.Join(values, ", "))

	result, err := s.pool.ExecContext(ctx, statement, args...)
	if err != nil {
		return 0, fmt.Errorf("notify/sqlstore: inserting: %w", err)
	}
	created, err := result.RowsAffected()
	if err != nil {
		// Reported rather than guessed. A driver that cannot count is not a
		// reason to claim everything was written.
		return 0, fmt.Errorf("notify/sqlstore: counting inserted rows: %w", err)
	}
	return int(created), nil
}

func (s *Store) Page(ctx context.Context, owner database.Owner, cur database.Cursor) ([]database.Row, database.Cursor, error) {
	limit := database.ClampLimit(cur.Limit)

	// One more than asked for, so the presence of a next page is known
	// without a second count.
	args := []any{owner.Kind, owner.ID}
	where := fmt.Sprintf("notifiable_type = %s AND notifiable_id = %s", s.ph(1), s.ph(2))
	if cur.BeforeID != "" {
		id, err := strconv.ParseInt(cur.BeforeID, 10, 64)
		if err != nil {
			// A cursor arrives from a query string, so a bad one is a request
			// problem rather than a server one.
			return nil, database.Cursor{}, fmt.Errorf("notify/sqlstore: %q is not a cursor", cur.BeforeID)
		}
		where += fmt.Sprintf(" AND id < %s", s.ph(3))
		args = append(args, id)
	}

	statement := fmt.Sprintf(
		`SELECT id, notifiable_type, notifiable_id, type, data, dedupe_key, read_at, created_at
   FROM %s WHERE %s ORDER BY id DESC LIMIT %d`, s.table, where, limit+1)

	queried, err := s.pool.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, database.Cursor{}, fmt.Errorf("notify/sqlstore: paging: %w", err)
	}
	defer queried.Close()

	rows := make([]database.Row, 0, limit)
	for queried.Next() {
		row, err := s.scan(queried)
		if err != nil {
			return nil, database.Cursor{}, err
		}
		rows = append(rows, row)
	}
	if err := queried.Err(); err != nil {
		return nil, database.Cursor{}, fmt.Errorf("notify/sqlstore: paging: %w", err)
	}

	var next database.Cursor
	if len(rows) > limit {
		rows = rows[:limit]
		next = database.Cursor{BeforeID: rows[len(rows)-1].ID, Limit: limit}
	}
	return rows, next, nil
}

func (s *Store) Unread(ctx context.Context, owner database.Owner) (int, error) {
	// The subquery's LIMIT is what makes this bounded forever: at most
	// MaxUnread+1 entries are read from the partial index whether the owner
	// has three unread or three hundred thousand, and the badge renders
	// "99+" past that anyway.
	statement := fmt.Sprintf(
		`SELECT count(*) FROM (
    SELECT 1 FROM %s
     WHERE notifiable_type = %s AND notifiable_id = %s AND read_at IS NULL
     LIMIT %d
) counted`, s.table, s.ph(1), s.ph(2), database.MaxUnread+1)

	var count int
	if err := s.pool.QueryRowContext(ctx, statement, owner.Kind, owner.ID).Scan(&count); err != nil {
		return 0, fmt.Errorf("notify/sqlstore: counting unread: %w", err)
	}
	return count, nil
}

func (s *Store) MarkRead(ctx context.Context, owner database.Owner, id string, at time.Time) (bool, error) {
	parsed, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		// Not an error: an id that cannot name a row names no row, which is
		// the same answer as somebody else's id.
		return false, nil
	}

	// The owner and read_at IS NULL are both in the condition, so the
	// affected row count is the answer. Two taps on one bell cannot both
	// report a change, and one person cannot mark another's notification read
	// even if a handler forgot to check.
	statement := fmt.Sprintf(
		`UPDATE %s SET read_at = %s
 WHERE id = %s AND notifiable_type = %s AND notifiable_id = %s AND read_at IS NULL`,
		s.table, s.ph(1), s.ph(2), s.ph(3), s.ph(4))

	result, err := s.pool.ExecContext(ctx, statement,
		s.writeTime(at.UTC()), parsed, owner.Kind, owner.ID)
	if err != nil {
		return false, fmt.Errorf("notify/sqlstore: marking read: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("notify/sqlstore: counting the update: %w", err)
	}
	return changed == 1, nil
}

func (s *Store) MarkAllRead(ctx context.Context, owner database.Owner, at time.Time) (int, error) {
	statement := fmt.Sprintf(
		`UPDATE %s SET read_at = %s
 WHERE notifiable_type = %s AND notifiable_id = %s AND read_at IS NULL`,
		s.table, s.ph(1), s.ph(2), s.ph(3))

	result, err := s.pool.ExecContext(ctx, statement,
		s.writeTime(at.UTC()), owner.Kind, owner.ID)
	if err != nil {
		return 0, fmt.Errorf("notify/sqlstore: marking all read: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("notify/sqlstore: counting the update: %w", err)
	}
	return int(changed), nil
}

func (s *Store) Delete(ctx context.Context, owner database.Owner, id string) (bool, error) {
	parsed, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return false, nil
	}

	statement := fmt.Sprintf(
		`DELETE FROM %s WHERE id = %s AND notifiable_type = %s AND notifiable_id = %s`,
		s.table, s.ph(1), s.ph(2), s.ph(3))

	result, err := s.pool.ExecContext(ctx, statement, parsed, owner.Kind, owner.ID)
	if err != nil {
		return false, fmt.Errorf("notify/sqlstore: deleting: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("notify/sqlstore: counting the delete: %w", err)
	}
	return removed == 1, nil
}

func (s *Store) Prune(ctx context.Context, before time.Time, limit int) (int, error) {
	if limit <= 0 {
		limit = database.MaxPageSize
	}

	// Batched through a subselect rather than a bare DELETE with a LIMIT,
	// which Postgres does not accept. An unread row is never taken, whatever
	// its age: somebody who has not looked in a year still has not looked.
	statement := fmt.Sprintf(
		`DELETE FROM %s WHERE id IN (
    SELECT id FROM %s
     WHERE read_at IS NOT NULL AND created_at < %s
     ORDER BY id LIMIT %d
)`, s.table, s.table, s.ph(1), limit)

	result, err := s.pool.ExecContext(ctx, statement, s.writeTime(before.UTC()))
	if err != nil {
		return 0, fmt.Errorf("notify/sqlstore: pruning: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("notify/sqlstore: counting the prune: %w", err)
	}
	return int(removed), nil
}

// scan reads one row.
func (s *Store) scan(queried *sql.Rows) (database.Row, error) {
	var (
		row       database.Row
		id        int64
		data      []byte
		dedupe    sql.NullString
		readAt    nullTime
		createdAt nullTime
	)
	if err := queried.Scan(&id, &row.Owner.Kind, &row.Owner.ID, &row.Type,
		&data, &dedupe, &readAt, &createdAt); err != nil {
		return database.Row{}, fmt.Errorf("notify/sqlstore: reading a row: %w", err)
	}

	row.ID = strconv.FormatInt(id, 10)
	// The bytes are carried through unchanged. Decoding and re-encoding would
	// reorder the keys, and the JSON is a contract with the client.
	row.Data = json.RawMessage(data)
	row.DedupeKey = dedupe.String
	if readAt.valid {
		when := readAt.at
		row.ReadAt = &when
	}
	row.CreatedAt = createdAt.at
	return row, nil
}

// nullTime scans a timestamp however the driver chose to hand it over.
//
// One scan target for both dialects, because they disagree: lib/pq returns a
// time.Time for TIMESTAMPTZ, while SQLite's column is TEXT and arrives as a
// string or a []byte depending on the driver. Scanning into sql.NullTime
// fails on the text, and sql.NullString fails on the time, so neither
// stdlib type can serve both — and a store that worked only on the dialect
// its tests happened to use is exactly what the contract suite exists to
// catch.
type nullTime struct {
	at    time.Time
	valid bool
}

func (n *nullTime) Scan(value any) error {
	switch typed := value.(type) {
	case nil:
		n.at, n.valid = time.Time{}, false
		return nil
	case time.Time:
		n.at, n.valid = typed.UTC(), true
		return nil
	case string:
		return n.parse(typed)
	case []byte:
		return n.parse(string(typed))
	default:
		return fmt.Errorf("notify/sqlstore: a timestamp arrived as %T, "+
			"which this store cannot read", value)
	}
}

func (n *nullTime) parse(value string) error {
	for _, layout := range []string{sqliteTimeLayout, time.RFC3339Nano, time.RFC3339} {
		if when, err := time.Parse(layout, value); err == nil {
			n.at, n.valid = when.UTC(), true
			return nil
		}
	}
	return fmt.Errorf("notify/sqlstore: %q is not a timestamp this store wrote", value)
}

// writeTime renders a timestamp for the column it is going into.
//
// Postgres takes a time.Time against TIMESTAMPTZ. SQLite's column is TEXT, so
// it takes a string — and the format has to sort lexicographically, because
// created_at < $1 is a string comparison there. RFC 3339 in UTC with a fixed
// width does; anything with a variable-width fraction does not.
func (s *Store) writeTime(at time.Time) any {
	if s.dialect == db.Postgres {
		return at
	}
	return at.UTC().Format(sqliteTimeLayout)
}

const sqliteTimeLayout = "2006-01-02 15:04:05.000000"
