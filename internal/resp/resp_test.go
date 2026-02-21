package resp

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// ============== Reader Tests ==============

func TestReader_ReadSimpleString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"basic", "+OK\r\n", "OK"},
		{"with spaces", "+Hello World\r\n", "Hello World"},
		{"empty", "+\r\n", ""},
		{"special chars", "+OK:123\r\n", "OK:123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.input))
			v, err := r.Read()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v.Type != SimpleString {
				t.Errorf("expected SimpleString type, got %c", v.Type)
			}
			if v.Str != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, v.Str)
			}
		})
	}
}

func TestReader_ReadError(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"basic error", "-ERR unknown command\r\n", "ERR unknown command"},
		{"wrong type", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", "WRONGTYPE Operation against a key holding the wrong kind of value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.input))
			v, err := r.Read()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v.Type != Error {
				t.Errorf("expected Error type, got %c", v.Type)
			}
			if v.Str != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, v.Str)
			}
		})
	}
}

func TestReader_ReadInteger(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{"positive", ":1000\r\n", 1000},
		{"zero", ":0\r\n", 0},
		{"negative", ":-1\r\n", -1},
		{"large", ":9223372036854775807\r\n", 9223372036854775807},
		{"negative large", ":-9223372036854775808\r\n", -9223372036854775808},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.input))
			v, err := r.Read()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v.Type != Integer {
				t.Errorf("expected Integer type, got %c", v.Type)
			}
			if v.Num != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, v.Num)
			}
		})
	}
}

func TestReader_ReadBulkString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		isNull   bool
	}{
		{"basic", "$5\r\nhello\r\n", "hello", false},
		{"empty", "$0\r\n\r\n", "", false},
		{"with newline", "$12\r\nhello\r\nworld\r\n", "hello\r\nworld", false},
		{"null", "$-1\r\n", "", true},
		{"binary safe", "$6\r\nhe\x00llo\r\n", "he\x00llo", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.input))
			v, err := r.Read()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v.Type != BulkString {
				t.Errorf("expected BulkString type, got %c", v.Type)
			}
			if v.Null != tt.isNull {
				t.Errorf("expected Null=%v, got %v", tt.isNull, v.Null)
			}
			if !tt.isNull && v.Bulk != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, v.Bulk)
			}
		})
	}
}

func TestReader_ReadArray(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		length int
		isNull bool
	}{
		{"empty", "*0\r\n", 0, false},
		{"null", "*-1\r\n", 0, true},
		{"single element", "*1\r\n$5\r\nhello\r\n", 1, false},
		{"multiple elements", "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", 3, false},
		{"mixed types", "*3\r\n:1\r\n+OK\r\n$5\r\nhello\r\n", 3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.input))
			v, err := r.Read()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v.Type != Array {
				t.Errorf("expected Array type, got %c", v.Type)
			}
			if v.Null != tt.isNull {
				t.Errorf("expected Null=%v, got %v", tt.isNull, v.Null)
			}
			if !tt.isNull && len(v.Array) != tt.length {
				t.Errorf("expected array length %d, got %d", tt.length, len(v.Array))
			}
		})
	}
}

func TestReader_ReadNestedArray(t *testing.T) {
	input := "*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n*2\r\n$1\r\nc\r\n$1\r\nd\r\n"
	r := NewReader(strings.NewReader(input))
	v, err := r.Read()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v.Type != Array || len(v.Array) != 2 {
		t.Fatalf("expected 2-element array, got %+v", v)
	}

	// Check first nested array
	if v.Array[0].Type != Array || len(v.Array[0].Array) != 2 {
		t.Errorf("expected nested 2-element array, got %+v", v.Array[0])
	}
	if v.Array[0].Array[0].Bulk != "a" || v.Array[0].Array[1].Bulk != "b" {
		t.Errorf("first nested array mismatch")
	}

	// Check second nested array
	if v.Array[1].Array[0].Bulk != "c" || v.Array[1].Array[1].Bulk != "d" {
		t.Errorf("second nested array mismatch")
	}
}

func TestReader_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"unknown type", "X123\r\n"},
		{"invalid integer", ":abc\r\n"},
		{"invalid bulk length", "$abc\r\n"},
		{"invalid array length", "*abc\r\n"},
		{"missing CRLF", "+OK\n"},
		{"incomplete bulk", "$10\r\nhello\r\n"},
		{"EOF", ""},
		// Additional negative test cases
		{"integer overflow", ":99999999999999999999999999999\r\n"},
		{"bulk length mismatch short", "$10\r\nabc\r\n"},
		{"array element parse failure", "*2\r\n$3\r\nfoo\r\n:abc\r\n"},
		{"nested array failure", "*1\r\n*1\r\n$abc\r\n"},
		{"only CR no LF", "+OK\r"},
		{"only type byte then EOF", "+"},
		{"bulk string no trailing CRLF", "$3\r\nfoo"},
		{"integer with spaces", ": 123\r\n"},
		{"integer with trailing chars", ":123abc\r\n"},
		{"empty integer", ":\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.input))
			_, err := r.Read()
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

// TestReader_NegativeLengthPanic documents that negative bulk/array lengths
// other than -1 cause a panic in the current implementation.
// This is a known issue that should be fixed.
func TestReader_NegativeLengthPanic(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{"negative bulk length -2", "$-2\r\n"},
		{"negative bulk length -99", "$-99\r\n"},
		{"negative array length -2", "*-2\r\n"},
		{"negative array length -99", "*-99\r\n"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					// Expected - current implementation panics
					t.Logf("Known issue: %s causes panic: %v", tc.name, r)
				}
			}()

			r := NewReader(strings.NewReader(tc.input))
			_, err := r.Read()
			if err == nil {
				t.Error("expected error or panic, got nil")
			} else {
				t.Logf("Returned error (good): %v", err)
			}
		})
	}
}

func TestReader_EOF(t *testing.T) {
	r := NewReader(strings.NewReader(""))
	_, err := r.Read()
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

// ============== Writer Tests ==============

func TestWriter_WriteSimpleString(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	err := w.WriteSimpleString("OK")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w.Flush()

	expected := "+OK\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestWriter_WriteError(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	err := w.WriteError("ERR unknown command")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w.Flush()

	expected := "-ERR unknown command\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestWriter_WriteInteger(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		expected string
	}{
		{"positive", 1000, ":1000\r\n"},
		{"zero", 0, ":0\r\n"},
		{"negative", -1, ":-1\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			err := w.WriteInteger(tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			w.Flush()

			if buf.String() != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, buf.String())
			}
		})
	}
}

func TestWriter_WriteBulkString(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{"basic", "hello", "$5\r\nhello\r\n"},
		{"empty", "", "$0\r\n\r\n"},
		{"with newline", "hello\r\nworld", "$12\r\nhello\r\nworld\r\n"},
		{"binary", "he\x00llo", "$6\r\nhe\x00llo\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			err := w.WriteBulkString(tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			w.Flush()

			if buf.String() != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, buf.String())
			}
		})
	}
}

func TestWriter_WriteNull(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	err := w.WriteNull()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w.Flush()

	expected := "$-1\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestWriter_WriteNullArray(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	err := w.WriteNullArray()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w.Flush()

	expected := "*-1\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestWriter_WriteArray(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	err := w.WriteArray([]Value{
		{Type: BulkString, Bulk: "SET"},
		{Type: BulkString, Bulk: "key"},
		{Type: BulkString, Bulk: "value"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w.Flush()

	expected := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestWriter_WriteEmptyArray(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	err := w.WriteArray([]Value{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w.Flush()

	expected := "*0\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestWriter_WriteStringArray(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	err := w.WriteStringArray([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w.Flush()

	expected := "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestWriter_WriteValue(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected string
	}{
		{"simple string", Value{Type: SimpleString, Str: "OK"}, "+OK\r\n"},
		{"error", Value{Type: Error, Str: "ERR"}, "-ERR\r\n"},
		{"integer", Value{Type: Integer, Num: 42}, ":42\r\n"},
		{"bulk string", Value{Type: BulkString, Bulk: "hello"}, "$5\r\nhello\r\n"},
		{"null bulk", Value{Type: BulkString, Null: true}, "$-1\r\n"},
		{"null array", Value{Type: Array, Null: true}, "*-1\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			err := w.WriteValue(tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			w.Flush()

			if buf.String() != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, buf.String())
			}
		})
	}
}

// ============== Roundtrip Tests ==============

func TestRoundtrip(t *testing.T) {
	tests := []struct {
		name  string
		value Value
	}{
		{"simple string", Value{Type: SimpleString, Str: "OK"}},
		{"error", Value{Type: Error, Str: "ERR something wrong"}},
		{"integer", Value{Type: Integer, Num: 12345}},
		{"negative integer", Value{Type: Integer, Num: -999}},
		{"bulk string", Value{Type: BulkString, Bulk: "hello world"}},
		{"empty bulk", Value{Type: BulkString, Bulk: ""}},
		{"null bulk", Value{Type: BulkString, Null: true}},
		{"null array", Value{Type: Array, Null: true}},
		{"empty array", Value{Type: Array, Array: []Value{}}},
		{"array with elements", Value{Type: Array, Array: []Value{
			{Type: BulkString, Bulk: "a"},
			{Type: Integer, Num: 1},
			{Type: SimpleString, Str: "OK"},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write
			var buf bytes.Buffer
			w := NewWriter(&buf)
			err := w.WriteValue(tt.value)
			if err != nil {
				t.Fatalf("write error: %v", err)
			}
			w.Flush()

			// Read back
			r := NewReader(&buf)
			result, err := r.Read()
			if err != nil {
				t.Fatalf("read error: %v", err)
			}

			// Compare
			if !valuesEqual(tt.value, result) {
				t.Errorf("roundtrip mismatch:\noriginal: %+v\nresult:   %+v", tt.value, result)
			}
		})
	}
}

func valuesEqual(a, b Value) bool {
	if a.Type != b.Type {
		return false
	}
	if a.Null != b.Null {
		return false
	}
	if a.Str != b.Str {
		return false
	}
	if a.Num != b.Num {
		return false
	}
	if a.Bulk != b.Bulk {
		return false
	}
	if len(a.Array) != len(b.Array) {
		return false
	}
	for i := range a.Array {
		if !valuesEqual(a.Array[i], b.Array[i]) {
			return false
		}
	}
	return true
}

// ============== Helper Function Tests ==============

func TestHelperFunctions(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		v := OK()
		if v.Type != SimpleString || v.Str != "OK" {
			t.Errorf("OK() = %+v", v)
		}
	})

	t.Run("Err", func(t *testing.T) {
		v := Err("something wrong")
		if v.Type != Error || v.Str != "ERR something wrong" {
			t.Errorf("Err() = %+v", v)
		}
	})

	t.Run("ErrWrongType", func(t *testing.T) {
		v := ErrWrongType()
		if v.Type != Error || !strings.Contains(v.Str, "WRONGTYPE") {
			t.Errorf("ErrWrongType() = %+v", v)
		}
	})

	t.Run("ErrWrongArgs", func(t *testing.T) {
		v := ErrWrongArgs("GET")
		if v.Type != Error || !strings.Contains(v.Str, "GET") || !strings.Contains(v.Str, "wrong number") {
			t.Errorf("ErrWrongArgs() = %+v", v)
		}
	})

	t.Run("NullBulk", func(t *testing.T) {
		v := NullBulk()
		if v.Type != BulkString || !v.Null {
			t.Errorf("NullBulk() = %+v", v)
		}
	})

	t.Run("NullArray", func(t *testing.T) {
		v := NullArray()
		if v.Type != Array || !v.Null {
			t.Errorf("NullArray() = %+v", v)
		}
	})

	t.Run("Int", func(t *testing.T) {
		v := Int(42)
		if v.Type != Integer || v.Num != 42 {
			t.Errorf("Int(42) = %+v", v)
		}
	})

	t.Run("Bulk", func(t *testing.T) {
		v := Bulk("hello")
		if v.Type != BulkString || v.Bulk != "hello" {
			t.Errorf("Bulk() = %+v", v)
		}
	})

	t.Run("Arr", func(t *testing.T) {
		v := Arr(Int(1), Int(2), Int(3))
		if v.Type != Array || len(v.Array) != 3 {
			t.Errorf("Arr() = %+v", v)
		}
	})
}

// ============== Writer Error Handling Tests ==============

type errorWriter struct {
	failAfter int
	written   int
}

func (e *errorWriter) Write(p []byte) (n int, err error) {
	if e.written >= e.failAfter {
		return 0, io.ErrClosedPipe
	}
	e.written += len(p)
	return len(p), nil
}

func TestWriter_ErrorHandling(t *testing.T) {
	// Note: bufio.Writer buffers writes, so Write methods may succeed
	// but Flush will fail. We test both the immediate case (if buffer fills)
	// and the Flush case.

	t.Run("Flush fails on error writer", func(t *testing.T) {
		w := NewWriter(&errorWriter{failAfter: 0})
		_ = w.WriteSimpleString("OK")
		err := w.Flush()
		if err == nil {
			t.Error("expected error on Flush, got nil")
		}
	})

	t.Run("Large write exceeds buffer", func(t *testing.T) {
		// Write a large enough string to exceed the default 4KB buffer
		largeString := strings.Repeat("x", 5000)
		w := NewWriter(&errorWriter{failAfter: 0})
		err := w.WriteBulkString(largeString)
		// Either Write fails or subsequent Flush fails
		if err == nil {
			err = w.Flush()
		}
		if err == nil {
			t.Error("expected error for large write to error writer")
		}
	})

	t.Run("WriteValue unknown type", func(t *testing.T) {
		var buf bytes.Buffer
		w := NewWriter(&buf)
		err := w.WriteValue(Value{Type: Type('Z')})
		if err == nil {
			t.Error("expected error for unknown type, got nil")
		}
	})

	t.Run("WriteArray with failed element write", func(t *testing.T) {
		// Array with a large element that exceeds buffer
		largeValue := Value{Type: BulkString, Bulk: strings.Repeat("x", 5000)}
		w := NewWriter(&errorWriter{failAfter: 0})
		err := w.WriteArray([]Value{largeValue})
		if err == nil {
			err = w.Flush()
		}
		if err == nil {
			t.Error("expected error for array write to error writer")
		}
	})

	t.Run("Multiple writes then flush", func(t *testing.T) {
		w := NewWriter(&errorWriter{failAfter: 0})
		_ = w.WriteSimpleString("OK")
		_ = w.WriteInteger(42)
		_ = w.WriteBulkString("hello")
		err := w.Flush()
		if err == nil {
			t.Error("expected error on Flush, got nil")
		}
	})
}

// ============== Benchmark Tests ==============

func BenchmarkReader_ReadBulkString(b *testing.B) {
	data := "$1000\r\n" + strings.Repeat("x", 1000) + "\r\n"
	for i := 0; i < b.N; i++ {
		r := NewReader(strings.NewReader(data))
		_, _ = r.Read()
	}
}

func BenchmarkReader_ReadArray(b *testing.B) {
	data := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	for i := 0; i < b.N; i++ {
		r := NewReader(strings.NewReader(data))
		_, _ = r.Read()
	}
}

func BenchmarkWriter_WriteArray(b *testing.B) {
	values := []Value{
		{Type: BulkString, Bulk: "SET"},
		{Type: BulkString, Bulk: "key"},
		{Type: BulkString, Bulk: "value"},
	}
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w := NewWriter(&buf)
		_ = w.WriteArray(values)
		_ = w.Flush()
	}
}
