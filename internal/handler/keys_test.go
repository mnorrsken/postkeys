package handler

import (
	"slices"
	"testing"

	"github.com/mnorrsken/postkeys/internal/resp"
)

func TestCommandKeys(t *testing.T) {
	cases := []struct {
		cmd  []string
		want []string
	}{
		{[]string{"SET", "k", "v", "EX", "10"}, []string{"k"}},
		{[]string{"PING"}, nil},
		{[]string{"DEL", "a", "b"}, []string{"a", "b"}},
		{[]string{"MSET", "a", "1", "b", "2"}, []string{"a", "b"}},
		{[]string{"RPOPLPUSH", "src", "dst"}, []string{"src", "dst"}},
		{[]string{"BLPOP", "a", "b", "0"}, []string{"a", "b"}},
		{[]string{"LMPOP", "2", "a", "b", "LEFT"}, []string{"a", "b"}},
		{[]string{"BZMPOP", "0", "1", "z", "MIN"}, []string{"z"}},
		{[]string{"ZUNIONSTORE", "dst", "2", "a", "b", "WEIGHTS", "1", "2"}, []string{"dst", "a", "b"}},
		{[]string{"BITOP", "AND", "dst", "a", "b"}, []string{"dst", "a", "b"}},
		{[]string{"EVALSHA", "sha", "1", "k", "arg"}, []string{"k"}},
		{[]string{"EVAL", "script", "5", "k"}, nil}, // numkeys larger than args
		{[]string{"GET"}, nil},
	}
	for _, c := range cases {
		args := make([]resp.Value, len(c.cmd)-1)
		for i, a := range c.cmd[1:] {
			args[i] = resp.Bulk(a)
		}
		if got := commandKeys(c.cmd[0], args); !slices.Equal(got, c.want) {
			t.Errorf("%v: got %v, want %v", c.cmd, got, c.want)
		}
	}
}
