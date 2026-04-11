// Package version provides build-time version information injected via ldflags.
package version

import "fmt"

// Build-time variables injected via -ldflags "-X github.com/AstralJaeger/beat-exporter/internal/version.Version=x.y.z"
var (
	Version   = "dev"
	Revision  = "unknown"
	Branch    = "unknown"
	BuildUser = "unknown"
	BuildDate = "unknown"
)

// Print returns a formatted version string.
func Print(program string) string {
	return fmt.Sprintf(
		"%s version=%s revision=%s branch=%s buildUser=%s buildDate=%s\n",
		program, Version, Revision, Branch, BuildUser, BuildDate,
	)
}
