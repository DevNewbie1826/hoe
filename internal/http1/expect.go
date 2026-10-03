package http1

// Go 1.27 closes a pending expectContinueReader when a final response suppresses
// its unsent continuation, and uses the underlying body's EOF for reuse.
// The pre-1.27 build overrides this during package initialization only.
var closeUnsentContinue = true
