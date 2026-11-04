package pubsub

import (
	"encoding/base64"
	"encoding/json"
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
		{encodeReference(42, "channel", 0), "channel", "", 42, false},
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

func TestReferencesRejectedByLegacyDecoder(t *testing.T) {
	for _, channel := range []string{"", "normal", strings.Repeat("\x00\xff", 8000)} {
		payload := encodeReference(42, channel, 0)
		if len(payload) >= 8000 {
			t.Fatalf("reference exceeds NOTIFY limit: %d", len(payload))
		}
		// This is the old decoder path: outer base64 followed by struct JSON.
		data, err := base64.StdEncoding.DecodeString(payload)
		if err == nil {
			var old struct {
				Channel string `json:"c"`
				Message string `json:"m"`
				Binary  bool   `json:"b"`
			}
			if json.Unmarshal(data, &old) == nil {
				t.Fatal("legacy decoder accepts reference as fake empty message")
			}
		}
		item, err := decodeDelivery(payload)
		if err != nil || item.id != 42 || item.key != channelKey(channel) {
			t.Fatalf("decode: %+v %v", item, err)
		}
		if len(channel) < 100 {
			if !item.known || item.channel != channel {
				t.Fatal("channel metadata missing")
			}
		} else if item.known {
			t.Fatal("oversized channel should use metadata query")
		}
	}
}

func TestMalformedVersionedReferences(t *testing.T) {
	key := channelKey("channel")
	for _, raw := range []string{
		`null`, `[]`, `{}`, `{"r":0,"k":"` + key + `"}`,
		`{"r":-1,"k":"` + key + `"}`, `{"r":"1","k":"` + key + `"}`,
		`{"r":1,"k":"xyz"}`, `{"r":1,"k":"` + key + `","c":"!"}`,
		`{"r":1,"k":"` + key + `","c":""}`, `{"r":1,"k":"` + key + `","m":"mixed"}`,
		`{"r":1,"k":"` + key + `"} {}`,
	} {
		if _, err := decodeReference(referencePrefix + raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}
func TestInlineAllBinaryBytes(t *testing.T) {
	binary := make([]byte, 256)
	for i := range binary {
		binary[i] = byte(i)
	}
	payload, err := encodeInline(string(binary), string(binary))
	if err != nil {
		t.Fatal(err)
	}
	channel, message, id, err := decodeBroadcast(payload)
	if err != nil || channel != string(binary) || message != string(binary) || id != 0 {
		t.Fatal("binary inline roundtrip failed")
	}
}

func TestReferenceHeaderSizeGuard(t *testing.T) {
	for _, size := range []int{8000, 512 << 20, int(^uint(0) >> 1)} {
		if referenceChannelFits(100, size) {
			t.Fatalf("channel size %d must use hash-only reference", size)
		}
	}
	if !referenceChannelFits(100, 3) {
		t.Fatal("short channel should fit reference metadata")
	}
}

func TestReferenceSizeValidation(t *testing.T) {
	key := channelKey("channel")
	for _, size := range []string{`-1`, `536870913`, `null`, `"1"`} {
		payload := referencePrefix + `{"r":1,"k":"` + key + `","n":7,"s":` + size + `}`
		if _, err := decodeReference(payload); err == nil {
			t.Fatalf("accepted invalid body size %s", size)
		}
	}
}

func TestReferenceChannelSizeValidation(t *testing.T) {
	key := channelKey("channel")
	for _, size := range []string{`-1`, `536870913`, `null`, `"1"`} {
		payload := referencePrefix + `{"r":1,"k":"` + key + `","s":0,"n":` + size + `}`
		if _, err := decodeReference(payload); err == nil {
			t.Fatalf("accepted invalid channel size %s", size)
		}
	}
	// Even valid bounds must agree with an embedded channel's actual bytes.
	payload := referencePrefix + `{"r":1,"k":"` + key + `","s":0,"n":0,"c":"Y2hhbm5lbA=="}`
	if _, err := decodeReference(payload); err == nil {
		t.Fatal("accepted embedded channel size mismatch")
	}
}
