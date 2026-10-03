//go:build !go1.27

package http1

// Go 1.25 and 1.26 only suppress the unsent continuation; body reads remain
// permitted, and reuse depends on EOF observed by the expect reader itself.
func init() { closeUnsentContinue = false }
