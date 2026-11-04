package pubsub

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestInlinePayloadBoundary(t *testing.T) {
	if inlinePayloadFits(512<<20, 512<<20) {
		t.Fatal("large inputs must not fit inline")
	}
	for _, n := range []int{0, 1, 2, 3, 4460, 4470, 4480, 6000, 8000} {
		for _, c := range []string{"", "abc", "\x00\xff"} {
			payload, err := encodeInline(c, strings.Repeat("x", n))
			if err != nil {
				t.Fatal(err)
			}
			if got, want := inlinePayloadFits(len(c), n), len(payload) < 8000; got != want {
				t.Fatalf("lengths %d,%d: got %v want %v (encoded %d)", len(c), n, got, want, len(payload))
			}
		}
	}
	for n := 4400; n < 4600; n++ {
		payload, _ := encodeInline("", strings.Repeat("x", n))
		if inlinePayloadFits(0, n) != (len(payload) < 8000) {
			t.Fatalf("boundary %d: %d", n, len(payload))
		}
	}
}

func TestBroadcastDecoding(t *testing.T) {
	inline, err := encodeInline("\x00\xff", "hello\x00")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		payload, channel, message string
		id                        int64
		bad                       bool
	}{
		{inline, "\x00\xff", "hello\x00", 0, false},
		{base64.StdEncoding.EncodeToString([]byte(`{"c":"legacy","m":"text"}`)), "legacy", "text", 0, false},
		{base64.StdEncoding.EncodeToString([]byte(`{"r":"42"}`)), "", "", 42, false},
	}
	for _, raw := range []string{`{"r":"0"}`, `{"r":"-1"}`, `{"r":"+1"}`, `{"r":1}`, `{"r":null}`, `{"r":"9223372036854775808"}`, `{"r":"1","c":"x"}`, `{"r":"1","extra":true}`, `{"r":"1","r":"2"}`} {
		cases = append(cases, struct {
			payload, channel, message string
			id                        int64
			bad                       bool
		}{base64.StdEncoding.EncodeToString([]byte(raw)), "", "", 0, true})
	}
	for _, tc := range cases {
		c, m, id, err := decodeBroadcast(tc.payload)
		if (err != nil) != tc.bad || (!tc.bad && (c != tc.channel || m != tc.message || id != tc.id)) {
			t.Errorf("decode %q = %q %q %d %v", tc.payload, c, m, id, err)
		}
	}
}

func TestMalformedBroadcast(t *testing.T) {
	for _, payload := range []string{
		"not base64!",
		base64.StdEncoding.EncodeToString([]byte(`null`)),
		base64.StdEncoding.EncodeToString([]byte(`[]`)),
		base64.StdEncoding.EncodeToString([]byte(`{`)),
	} {
		if _, _, _, err := decodeBroadcast(payload); err == nil {
			t.Errorf("accepted malformed payload %q", payload)
		}
	}
}

func TestInlineWireFormat(t *testing.T) {
	got, err := encodeInline("a", "b")
	if err != nil {
		t.Fatal(err)
	}
	want := base64.StdEncoding.EncodeToString([]byte(`{"c":"YQ==","m":"Yg==","b":true}`))
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
