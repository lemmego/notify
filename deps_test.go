package notify_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestRootIsStandardLibraryOnly pins this package's dependency set.
//
// The lesson is lemmego/log's: with its provider inside the module, a
// consumer importing only the core inherited 289 modules — cloud SDKs,
// OpenTelemetry, gRPC — for a package that formats text. Moving the provider
// out took it to 2.
//
// So the rule here is stricter than "no mail, no SQL": the root may import
// nothing outside the standard library at all. Even api/app is too much,
// because api/app reaches api/db and therefore a Postgres driver — which the
// first version of this test caught, and which is why notify.Provider lives
// in notify/provider instead.
//
// A failure means something that needs the container, a transport or a driver
// has been declared in the root. It belongs beside the channel that needs it,
// where it costs only the applications that wire that channel.
func TestRootIsStandardLibraryOnly(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("listing dependencies: %v", err)
	}

	for _, dep := range strings.Fields(string(out)) {
		if dep == "github.com/lemmego/notify" {
			continue
		}
		// A standard-library path has no dot in its first segment, which is
		// how the toolchain itself tells the two apart.
		first, _, _ := strings.Cut(dep, "/")
		if strings.Contains(first, ".") {
			t.Errorf("the root package depends on %s, which is outside the "+
				"standard library.\nEvery channel owns its own optional "+
				"interfaces and its own dependencies, so an application that "+
				"only wants in-app notifications compiles no SMTP client and no "+
				"database driver. Move the declaration that pulled this in "+
				"beside its channel, or into notify/provider.", dep)
		}
	}
}
