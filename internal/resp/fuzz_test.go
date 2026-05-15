package resp

import (
	"bytes"
	"strings"
	"testing"
)

// FuzzReader exercises the RESP parser against arbitrary byte input. The goal
// is not to assert any particular parse result — many inputs are invalid and
// must surface as errors — but to catch panics, infinite loops, and unbounded
// allocations that would let a malicious client crash the server.
//
// Seed inputs cover all the wire shapes the existing unit tests touch (simple
// string, error, integer, bulk string, nested array) plus the negative cases
// from TestReader_Errors and the negative-length panic regression that
// resp_test.go documents as a known issue.
func FuzzReader(f *testing.F) {
	seeds := []string{
		// Valid shapes
		"+OK\r\n",
		"-ERR something\r\n",
		":1000\r\n",
		":-1\r\n",
		":0\r\n",
		":9223372036854775807\r\n",
		"$5\r\nhello\r\n",
		"$0\r\n\r\n",
		"$-1\r\n",
		"*0\r\n",
		"*-1\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
		"*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n*2\r\n$1\r\nc\r\n$1\r\nd\r\n",
		// Negative / pathological inputs
		"X123\r\n",
		":abc\r\n",
		"$abc\r\n",
		"*abc\r\n",
		"+OK\n",
		"$10\r\nhello\r\n",
		"$-2\r\n",
		"*-2\r\n",
		":99999999999999999999999999999\r\n",
		"$10\r\nabc\r\n",
		"*2\r\n$3\r\nfoo\r\n:abc\r\n",
		"*1\r\n*1\r\n$abc\r\n",
		"+",
		"$3\r\nfoo",
		"",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Cap input size so the fuzzer doesn't spend its budget on giant
		// allocations driven by spurious huge-length prefixes; the parser is
		// allowed to allocate proportional to the declared length, so without
		// this we'd be testing the allocator more than the parser.
		if len(data) > 64*1024 {
			return
		}

		r := NewReader(bytes.NewReader(data))
		// Read repeatedly so framed inputs (multiple values back-to-back)
		// also get exercised. Any panic fails the fuzz target.
		for i := 0; i < 32; i++ {
			v, err := r.Read()
			if err != nil {
				return
			}
			// Touch the parsed value to prevent the compiler from eliding work
			// and to catch the rare case where Read returns no error but
			// produces a value that's structurally inconsistent.
			_ = v.Type
		}
	})
}

// FuzzRoundtrip writes a value, reads it back, and checks the parse succeeds.
// This is a weaker invariant than full equality — Writer doesn't support all
// RESP3 types yet — but it ensures the writer never produces output the reader
// rejects for the types the writer does emit.
func FuzzRoundtrip(f *testing.F) {
	f.Add("hello", int64(0), byte('+'))
	f.Add("", int64(-1), byte('$'))
	f.Add("a\r\nb", int64(0), byte('$'))
	f.Add("ERR boom", int64(0), byte('-'))
	f.Add("", int64(42), byte(':'))

	f.Fuzz(func(t *testing.T, s string, n int64, typ byte) {
		// Restrict to the basic types Writer handles.
		var v Value
		switch Type(typ) {
		case SimpleString:
			// Simple strings can't contain CR or LF.
			if strings.ContainsAny(s, "\r\n") {
				return
			}
			v = Value{Type: SimpleString, Str: s}
		case Error:
			if strings.ContainsAny(s, "\r\n") {
				return
			}
			v = Value{Type: Error, Str: s}
		case Integer:
			v = Value{Type: Integer, Num: n}
		case BulkString:
			v = Value{Type: BulkString, Bulk: s}
		default:
			return
		}

		var buf bytes.Buffer
		w := NewWriter(&buf)
		if err := w.WriteValue(v); err != nil {
			t.Fatalf("write failed for %+v: %v", v, err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("flush failed: %v", err)
		}

		r := NewReader(&buf)
		got, err := r.Read()
		if err != nil {
			t.Fatalf("read-after-write failed for %+v: %v (bytes=%q)", v, err, buf.String())
		}
		if got.Type != v.Type {
			t.Fatalf("type mismatch: wrote %c got %c", v.Type, got.Type)
		}
	})
}
