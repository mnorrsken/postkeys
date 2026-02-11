package cache

import (
	"testing"
)

func TestMatchGlob(t *testing.T) {
	tests := []struct {
		pattern string
		str     string
		match   bool
	}{
		{"pubsub:*", "pubsub:channel", true},
		{"pubsub:*", "pubsub:", true},
		{"pubsub:*", "other:channel", false},
		{"*:temp", "session:temp", true},
		{"*:temp", "session:permanent", false},
		{"prefix*suffix", "prefixmiddlesuffix", true},
		{"prefix*suffix", "prefixsuffix", true},
		{"prefix*suffix", "prefixmiddle", false},
		{"exact", "exact", true},
		{"exact", "notexact", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"/"+tt.str, func(t *testing.T) {
			result := matchGlob(tt.pattern, tt.str)
			if result != tt.match {
				t.Errorf("matchGlob(%q, %q) = %v, want %v", tt.pattern, tt.str, result, tt.match)
			}
		})
	}
}

func TestMatchesAnyPattern(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		patterns []string
		match    bool
	}{
		{"matches first pattern", "pubsub:channel1", []string{"pubsub:*", "lock:*"}, true},
		{"matches second pattern", "lock:mylock", []string{"pubsub:*", "lock:*"}, true},
		{"no match", "user:123", []string{"pubsub:*", "lock:*"}, false},
		{"empty patterns", "anything", nil, false},
		{"suffix match", "session:temp", []string{"*:temp"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesAnyPattern(tt.key, tt.patterns)
			if result != tt.match {
				t.Errorf("matchesAnyPattern(%q, %v) = %v, want %v", tt.key, tt.patterns, result, tt.match)
			}
		})
	}
}
