package database_test

import (
	"testing"

	"github.com/lemmego/notify/channel/database"
	"github.com/lemmego/notify/channel/database/storetest"
)

// The memory store is held to the same contract as the SQL one, because it is
// what every application's handler tests run against. A memory store that is
// laxer than production makes the suite green and the deployment wrong.
func TestMemoryStoreSatisfiesTheContract(t *testing.T) {
	storetest.Run(t, func(*testing.T) database.Store {
		return database.NewMemoryStore()
	})
}
