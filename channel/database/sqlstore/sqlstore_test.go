package sqlstore_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/glebarez/go-sqlite"

	"github.com/lemmego/api/db"
	"github.com/lemmego/notify/channel/database"
	"github.com/lemmego/notify/channel/database/sqlstore"
)

// The SQL store is held to the same contract as the memory one. Two
// implementations held to one contract is the only cheap way to know the
// store every handler test runs against behaves like the database every user
// hits.
func TestSQLiteStoreSatisfiesTheContract(t *testing.T) {
	storetestRun(t, db.SQLite, func(t *testing.T) *sql.DB {
		// A file rather than :memory:, because the contract's concurrency
		// case opens several connections and an in-memory SQLite database is
		// per-connection unless shared explicitly.
		pool, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "notify.sqlite"))
		if err != nil {
			t.Fatalf("opening sqlite: %v", err)
		}
		t.Cleanup(func() { pool.Close() })
		return pool
	})
}

func TestADialectWithNoStatementsIsRefusedByName(t *testing.T) {
	pool, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "notify.sqlite"))
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer pool.Close()

	// MySQL is deliberately absent. An untested dialect accepted here would
	// look like support right up to the point where the partial index failed
	// inside somebody's migration.
	_, err = sqlstore.NewWithPool(pool, db.MySQL)
	if err == nil {
		t.Fatal("a dialect with no statements was accepted")
	}
	if got := err.Error(); !contains(got, "postgres") || !contains(got, "sqlite") {
		t.Fatalf("the refusal does not name the dialects it has: %v", err)
	}
}

func TestTheTableCanBeMoved(t *testing.T) {
	pool, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "notify.sqlite"))
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer pool.Close()

	store, err := sqlstore.NewWithPool(pool, db.SQLite, sqlstore.WithTable("app_notifications"))
	if err != nil {
		t.Fatalf("building the store: %v", err)
	}
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("migrating: %v", err)
	}

	if _, err := store.Insert(context.Background(), []database.Row{{
		Owner: database.Owner{Kind: "user", ID: "1"},
		Type:  "followed",
	}}); err != nil {
		t.Fatalf("inserting into the moved table: %v", err)
	}

	var count int
	if err := pool.QueryRow(`SELECT count(*) FROM app_notifications`).Scan(&count); err != nil {
		t.Fatalf("reading the moved table: %v", err)
	}
	if count != 1 {
		t.Fatalf("got %d rows in app_notifications, want 1", count)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}
