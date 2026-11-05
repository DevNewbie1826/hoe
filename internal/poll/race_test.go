//go:build race

package poll

// raceEnabled reports whether the binary was built with the race detector,
// whose instrumentation allocates on every poll operation and would mask the
// steady-state allocation count this package asserts.
const raceEnabled = true
