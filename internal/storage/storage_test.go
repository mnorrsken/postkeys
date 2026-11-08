package storage

import "testing"

func TestServerMajor(t *testing.T) {
	tests := []struct {
		in   string
		want int
	}{
		{"18.1 (Debian 18.1-1.pgdg13+2)", 18},
		{"17.6", 17},
		{"16.10", 16},
		{"19beta1", 19},
		{"", 0},
	}
	for _, tt := range tests {
		if got := serverMajor(tt.in); got != tt.want {
			t.Errorf("serverMajor(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
