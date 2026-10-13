package notifytest_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/lemmego/notify"
	"github.com/lemmego/notify/notifytest"
)

// A test helper has to be tested against the thing it claims to catch, or it
// only tests itself. These assert that Contract fails on a deliberately
// broken pairing — the whole reason it exists.

type followed struct{}

func (followed) Via(notify.Notifiable) []string { return []string{"database"} }

type replied struct{}

// Names mail as well, which the recorder below will refuse — standing in for
// a notification that forgot ToMail.
func (replied) Via(notify.Notifiable) []string { return []string{"database", "mail"} }

func TestContractPassesWhenEveryPairingIsSatisfied(t *testing.T) {
	n := notify.New(notifytest.NewRecorder("database"))

	notifytest.Contract(t, n, notifytest.User{ID: "1"}, followed{})
}

func TestContractFailsWhenANotificationCannotBeDelivered(t *testing.T) {
	mail := notifytest.NewRecorder("mail")
	mail.Refuse("does not implement mail.Mailable (missing ToMail)")
	n := notify.New(notifytest.NewRecorder("database"), mail)

	fake := &recordingT{}
	notifytest.Contract(fake, n, notifytest.User{ID: "1"}, replied{})

	if !fake.failed {
		t.Fatal("Contract passed a notification that cannot be delivered")
	}
	if !strings.Contains(fake.log, "ToMail") {
		t.Fatalf("the failure does not say what is missing: %s", fake.log)
	}
}

func TestContractFailsWhenAChannelIsNotWired(t *testing.T) {
	n := notify.New(notifytest.NewRecorder("database"))

	fake := &recordingT{}
	// replied names mail, which is not wired — the typo case.
	notifytest.Contract(fake, n, notifytest.User{ID: "1"}, replied{})

	if !fake.failed {
		t.Fatal("Contract passed a notification naming an unwired channel")
	}
	if !strings.Contains(fake.log, "database") {
		t.Fatalf("the failure does not name what is wired: %s", fake.log)
	}
}

// An empty list would pass vacuously, which is worse than having no test —
// and it is exactly what happens when somebody forgets to populate All().
func TestContractRefusesAnEmptyList(t *testing.T) {
	n := notify.New(notifytest.NewRecorder("database"))

	fake := &recordingT{}
	notifytest.Contract(fake, n, notifytest.User{ID: "1"})

	if !fake.failed {
		t.Fatal("Contract passed with no notifications to check")
	}
	if !strings.Contains(fake.log, "vacuously") {
		t.Fatalf("the failure does not explain why an empty list is refused: %s", fake.log)
	}
}

func TestAssertSentFindsTheNotification(t *testing.T) {
	recorder := notifytest.NewRecorder("database")
	n := notify.New(recorder)
	ada := notifytest.User{ID: "1"}

	if err := n.Send(t.Context(), ada, followed{}); err != nil {
		t.Fatalf("sending: %v", err)
	}

	recorder.AssertSent[followed](t, ada)
	recorder.AssertNotSent[replied](t, ada)
	recorder.AssertNotSent[followed](t, notifytest.User{ID: "2"})
}

// Twice is a real bug with a real cause — a call site firing on a repeated
// upsert — so the assertion is exact rather than at-least-one.
func TestAssertSentRefusesTwoDeliveries(t *testing.T) {
	recorder := notifytest.NewRecorder("database")
	n := notify.New(recorder)
	ada := notifytest.User{ID: "1"}

	for i := 0; i < 2; i++ {
		if err := n.Send(t.Context(), ada, followed{}); err != nil {
			t.Fatalf("sending: %v", err)
		}
	}

	fake := &recordingT{}
	recorder.AssertSent[followed](fake, ada)
	if !fake.failed {
		t.Fatal("AssertSent accepted two deliveries of one notification")
	}
	if !strings.Contains(fake.log, "want once") {
		t.Fatalf("the failure does not say the count was wrong: %s", fake.log)
	}
}

// recordingT captures what a failing assertion reported, so the helper can be
// tested without failing the test that is testing it.
type recordingT struct {
	testing.TB
	failed bool
	log    string
}

func (r *recordingT) Helper() {}

func (r *recordingT) Errorf(format string, args ...any) {
	r.failed = true
	r.log += sprintf(format, args...)
}

func (r *recordingT) Fatalf(format string, args ...any) {
	r.failed = true
	r.log += sprintf(format, args...)
}

func (r *recordingT) Fatal(args ...any) {
	r.failed = true
	r.log += sprintln(args...)
}

func (r *recordingT) Error(args ...any) {
	r.failed = true
	r.log += sprintln(args...)
}

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }
func sprintln(args ...any) string               { return fmt.Sprintln(args...) }
