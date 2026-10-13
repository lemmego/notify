package sqlstore_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"

	"github.com/lemmego/api/db"
)

// SQLite proves the logic; Postgres proves the SQL. Only this test exercises
// $N placeholder numbering, a jsonb column, TIMESTAMPTZ arriving back as a
// time.Time rather than a string, and ON CONFLICT inferring a *partial*
// unique index — none of which SQLite can tell us anything about, and the
// last of which is what makes deduplication atomic.
//
// Skipped without a DSN, so the suite still runs on a machine with no
// Postgres:
//
//	NOTIFY_POSTGRES_DSN="postgres://$USER@127.0.0.1:5432/postgres?sslmode=disable" \
//	    go test ./channel/database/sqlstore/
func TestPostgresStoreSatisfiesTheContract(t *testing.T) {
	dsn := os.Getenv("NOTIFY_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set NOTIFY_POSTGRES_DSN to run the Postgres contract")
	}

	storetestRun(t, db.Postgres, func(t *testing.T) *sql.DB {
		admin, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Fatalf("opening postgres: %v", err)
		}
		defer admin.Close()

		// A database per case, not a schema and not a transaction: the
		// concurrency case needs several connections contending for one
		// unique index, so it cannot share a transaction with anything.
		name := fmt.Sprintf("notify_test_%d_%s", os.Getpid(), sanitise(t.Name()))
		if _, err := admin.Exec(`DROP DATABASE IF EXISTS ` + name); err != nil {
			t.Fatalf("dropping a stale scratch database: %v", err)
		}
		if _, err := admin.Exec(`CREATE DATABASE ` + name); err != nil {
			t.Fatalf("creating a scratch database: %v", err)
		}

		pool, err := sql.Open("postgres", replaceDatabase(dsn, name))
		if err != nil {
			t.Fatalf("opening the scratch database: %v", err)
		}
		t.Cleanup(func() {
			pool.Close()
			// Reopened, because the admin handle above is already closed by
			// the time this runs.
			cleanup, err := sql.Open("postgres", dsn)
			if err != nil {
				return
			}
			defer cleanup.Close()
			cleanup.Exec(`DROP DATABASE IF EXISTS ` + name)
		})
		return pool
	})
}

// sanitise turns a subtest name into something that can be a database name.
func sanitise(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+32)
		default:
			out = append(out, '_')
		}
	}
	// Postgres caps identifiers at 63 bytes and the prefix takes some.
	if len(out) > 40 {
		out = out[len(out)-40:]
	}
	return string(out)
}

// replaceDatabase points a DSN at another database on the same server.
func replaceDatabase(dsn, name string) string {
	slash := -1
	question := len(dsn)
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '?' && question == len(dsn) {
			question = i
		}
		if dsn[i] == '/' {
			slash = i
			break
		}
	}
	if slash < 0 {
		return dsn
	}
	return dsn[:slash+1] + name + dsn[question:]
}
