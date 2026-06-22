package bus

import "strings"

// matchSubject reports whether a NATS-style subscription pattern matches a delivered
// subject. Tokens are dot-separated: "*" matches exactly one token, ">" matches one or
// more trailing tokens (only valid as the final token). An exact pattern matches itself.
// This lets the in-process ZMQ dispatch honor the same wildcards the HTTP stream uses
// (e.g. /v1/stream subscribes to "swarm.>").
func matchSubject(pattern, subject string) bool {
	if pattern == subject {
		return true
	}
	pt := strings.Split(pattern, ".")
	st := strings.Split(subject, ".")
	for i, tok := range pt {
		if tok == ">" {
			// ">" must be final and needs at least one remaining subject token.
			return i == len(pt)-1 && i < len(st)
		}
		if i >= len(st) {
			return false
		}
		if tok != "*" && tok != st[i] {
			return false
		}
	}
	// All pattern tokens consumed: match only if the subject had no extra tokens.
	return len(pt) == len(st)
}
