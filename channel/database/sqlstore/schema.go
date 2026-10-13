// Package sqlstore keeps notifications in the application's own database.
//
// It takes an api/db.Connection rather than opening anything, so it shares
// the pool the application's ORM already uses — orm.Open wraps a *sql.DB it
// is handed, and both ormconnector and gormconnector register that pool as
// the db.Connection — and so the dialect travels with the connection instead
// of being re-derived from configuration, which is how migration and queue
// have disagreed before now.
//
// The statements are hand-written rather than going through the ORM, for
// three reasons. The ORM names every mapped column in its INSERT, so a
// jsonb DEFAULT and a created_at DEFAULT would both be overwritten with Go's
// zero values. ON CONFLICT against a partial unique index — which is what
// makes deduplication atomic rather than a check followed by a race — is not
// expressible through orm or gpa. And this is seven transitions, not CRUD on
// an entity, which is the shape a repository abstraction is for.
package sqlstore

import (
	"fmt"

	"github.com/lemmego/api/db"
)

// DefaultTable is where notifications live unless a prefix moves them.
const DefaultTable = "notifications"

// SchemaStatements returns the DDL for one dialect, in order.
//
// Returns nil for a dialect this package has no statements for, so a caller
// can refuse rather than run SQL nobody has tried. MySQL is absent
// deliberately: an untested dialect string is worse than an honest refusal,
// because it looks like support right up to the point where somebody's JSON
// column or partial index fails inside their migration.
func SchemaStatements(dialect db.Dialect, table string) []string {
	if table == "" {
		table = DefaultTable
	}
	switch dialect {
	case db.Postgres:
		return postgresSchema(table)
	case db.SQLite:
		return sqliteSchema(table)
	default:
		return nil
	}
}

func postgresSchema(table string) []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id              BIGSERIAL    PRIMARY KEY,
    notifiable_type VARCHAR(64)  NOT NULL,
    notifiable_id   VARCHAR(64)  NOT NULL,
    type            VARCHAR(191) NOT NULL,
    data            JSONB        NOT NULL DEFAULT '{}'::jsonb,
    dedupe_key      VARCHAR(128),
    read_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
)`, table),

		// Owner plus id, which serves both the newest-first page and the
		// keyset step. Plain id rather than id DESC: a btree scans backwards
		// as cheaply as forwards, so DESC adds nothing but a question for
		// whoever reads this next.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_owner_idx
    ON %s (notifiable_type, notifiable_id, id)`, table, table),

		// Partial, so the index's entire size is the number of unread
		// notifications in the system. That is what makes the badge's capped
		// count O(1) in the worst case rather than proportional to how busy
		// an account is.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_unread_idx
    ON %s (notifiable_type, notifiable_id)
 WHERE read_at IS NULL`, table, table),

		// The second defence against a duplicate notification, after the call
		// site. Partial so rows without a key never collide with each other —
		// an unconditional unique index would let one keyless notification
		// suppress every other keyless one for the same owner.
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s_dedupe_idx
    ON %s (notifiable_type, notifiable_id, dedupe_key)
 WHERE dedupe_key IS NOT NULL`, table, table),

		// Prune reads this one: read rows, oldest first.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_prunable_idx
    ON %s (created_at)
 WHERE read_at IS NOT NULL`, table, table),
	}
}

func sqliteSchema(table string) []string {
	return []string{
		// TEXT for the timestamps and a json_valid CHECK for the payload,
		// which is this dialect's norm. SQLite is here so the contract suite
		// runs in-process on every `go test`; Postgres is the target.
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    notifiable_type TEXT NOT NULL,
    notifiable_id   TEXT NOT NULL,
    type            TEXT NOT NULL,
    data            TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(data)),
    dedupe_key      TEXT,
    read_at         TEXT,
    created_at      TEXT NOT NULL
)`, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_owner_idx
    ON %s (notifiable_type, notifiable_id, id)`, table, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_unread_idx
    ON %s (notifiable_type, notifiable_id) WHERE read_at IS NULL`, table, table),
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s_dedupe_idx
    ON %s (notifiable_type, notifiable_id, dedupe_key)
 WHERE dedupe_key IS NOT NULL`, table, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_prunable_idx
    ON %s (created_at) WHERE read_at IS NOT NULL`, table, table),
	}
}
