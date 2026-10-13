package sqlstore_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/lemmego/api/db"
	"github.com/lemmego/notify/channel/database"
	"github.com/lemmego/notify/channel/database/sqlstore"
	"github.com/lemmego/notify/channel/database/storetest"
)

// storetestRun wires a pool into the shared contract suite.
//
// Each case gets its own database rather than its own transaction: the
// concurrency case needs several connections to contend for one unique
// index, which is the whole point of it.
func storetestRun(t *testing.T, dialect db.Dialect, open func(*testing.T) *sql.DB) {
	t.Helper()
	storetest.Run(t, func(t *testing.T) database.Store {
		pool := open(t)
		store, err := newStore(pool, dialect)
		if err != nil {
			t.Fatalf("building the store: %v", err)
		}
		if err := store.Migrate(context.Background()); err != nil {
			t.Fatalf("migrating: %v", err)
		}
		return store
	})
}

type migratable interface {
	database.Store
	Migrate(context.Context) error
}

func newStore(pool *sql.DB, dialect db.Dialect) (migratable, error) {
	return sqlstore.NewWithPool(pool, dialect)
}
