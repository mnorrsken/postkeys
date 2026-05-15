package server

import (
	"testing"

	"github.com/mnorrsken/postkeys/internal/resp"
)

func bulk(s string) resp.Value {
	return resp.Value{Type: resp.BulkString, Bulk: s}
}

func array(vs ...resp.Value) resp.Value {
	return resp.Value{Type: resp.Array, Array: vs}
}

func TestRedactSensitiveArgs(t *testing.T) {
	cases := []struct {
		name string
		in   resp.Value
		want []string
	}{
		{
			name: "AUTH password",
			in:   array(bulk("AUTH"), bulk("hunter2")),
			want: []string{"AUTH", "<redacted>"},
		},
		{
			name: "AUTH username password",
			in:   array(bulk("AUTH"), bulk("alice"), bulk("hunter2")),
			want: []string{"AUTH", "<redacted>", "<redacted>"},
		},
		{
			name: "AUTH lowercase",
			in:   array(bulk("auth"), bulk("hunter2")),
			want: []string{"auth", "<redacted>"},
		},
		{
			name: "HELLO with AUTH",
			in:   array(bulk("HELLO"), bulk("2"), bulk("AUTH"), bulk("alice"), bulk("hunter2")),
			want: []string{"HELLO", "2", "AUTH", "<redacted>", "<redacted>"},
		},
		{
			name: "HELLO with AUTH and SETNAME",
			in:   array(bulk("HELLO"), bulk("3"), bulk("AUTH"), bulk("alice"), bulk("hunter2"), bulk("SETNAME"), bulk("client1")),
			want: []string{"HELLO", "3", "AUTH", "<redacted>", "<redacted>", "SETNAME", "client1"},
		},
		{
			name: "HELLO without AUTH is unchanged",
			in:   array(bulk("HELLO"), bulk("3")),
			want: []string{"HELLO", "3"},
		},
		{
			name: "GET is unchanged",
			in:   array(bulk("GET"), bulk("mykey")),
			want: []string{"GET", "mykey"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactSensitiveArgs(tc.in)
			if len(got.Array) != len(tc.want) {
				t.Fatalf("length mismatch: got %d want %d (%+v)", len(got.Array), len(tc.want), got.Array)
			}
			for i, w := range tc.want {
				if got.Array[i].Bulk != w {
					t.Errorf("arg %d: got %q want %q", i, got.Array[i].Bulk, w)
				}
			}
		})
	}
}

func TestRedactSensitiveArgs_DoesNotMutateInput(t *testing.T) {
	in := array(bulk("AUTH"), bulk("hunter2"))
	_ = redactSensitiveArgs(in)
	if in.Array[1].Bulk != "hunter2" {
		t.Fatalf("input was mutated: %q", in.Array[1].Bulk)
	}
}
