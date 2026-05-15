// Package resp implements the Redis Serialization Protocol (RESP3).
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
)

// Limits guard against malicious or malformed inputs that declare absurd
// lengths up front; without these, a single command like "*9223372036854775807"
// would let the parser try to allocate a slice large enough to OOM the
// process before any element bytes have been read.
const (
	// MaxBulkLen matches the Redis default for `proto-max-bulk-len` (512 MiB).
	MaxBulkLen = 512 * 1024 * 1024
	// MaxArrayLen caps the number of elements a single array (or push/map)
	// can declare. 1 MiB elements is well above any legitimate command size
	// and keeps worst-case allocation bounded.
	MaxArrayLen = 1 << 20
)

// Value types in RESP protocol
type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
	Null         Type = '_'
	Boolean      Type = '#'
	Double       Type = ','
	BigNumber    Type = '('
	BulkError    Type = '!'
	VerbatimStr  Type = '='
	Map          Type = '%'
	Set          Type = '~'
	Push         Type = '>'
)

// Value represents a RESP value
type Value struct {
	Type    Type
	Str     string
	Num     int64
	Bulk    string
	Array   []Value
	MapData map[string]string // For RESP3 Map type
	Null    bool
}

// Reader reads RESP values from an io.Reader
type Reader struct {
	reader *bufio.Reader
	debug  bool
}

// NewReader creates a new RESP reader
func NewReader(r io.Reader) *Reader {
	return &Reader{reader: bufio.NewReader(r), debug: false}
}

// NewReaderWithDebug creates a new RESP reader with debug logging enabled
func NewReaderWithDebug(r io.Reader, debug bool) *Reader {
	return &Reader{reader: bufio.NewReader(r), debug: debug}
}

// SetDebug enables or disables debug logging
func (r *Reader) SetDebug(debug bool) {
	r.debug = debug
}

func (r *Reader) debugLog(format string, v ...interface{}) {
	if r.debug {
		log.Printf("[RESP DEBUG] "+format, v...)
	}
}

// Read reads a single RESP value
func (r *Reader) Read() (Value, error) {
	typeByte, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch Type(typeByte) {
	case SimpleString:
		return r.readSimpleString()
	case Error:
		return r.readError()
	case Integer:
		return r.readInteger()
	case BulkString:
		return r.readBulkString()
	case Array:
		return r.readArray()
	default:
		if r.debug {
			// Peek ahead to get the rest of the line for debugging
			peek, _ := r.reader.Peek(min(256, r.reader.Buffered()))
			r.debugLog("unknown RESP type byte: 0x%02x (%q), remaining buffer: %q", typeByte, string(typeByte), string(peek))
		}
		return Value{}, fmt.Errorf("unknown RESP type: %c", typeByte)
	}
}

func (r *Reader) readLine() (string, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", errors.New("invalid RESP line ending")
	}
	return line[:len(line)-2], nil
}

func (r *Reader) readSimpleString() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	return Value{Type: SimpleString, Str: line}, nil
}

func (r *Reader) readError() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	return Value{Type: Error, Str: line}, nil
}

func (r *Reader) readInteger() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	num, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, err
	}
	return Value{Type: Integer, Num: num}, nil
}

func (r *Reader) readBulkString() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return Value{}, err
	}

	if length == -1 {
		return Value{Type: BulkString, Null: true}, nil
	}
	if length < 0 {
		return Value{}, fmt.Errorf("invalid bulk string length: %d", length)
	}
	if length > MaxBulkLen {
		return Value{}, fmt.Errorf("bulk string length %d exceeds limit %d", length, MaxBulkLen)
	}

	buf := make([]byte, length+2)
	_, err = io.ReadFull(r.reader, buf)
	if err != nil {
		return Value{}, err
	}

	return Value{Type: BulkString, Bulk: string(buf[:length])}, nil
}

func (r *Reader) readArray() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}

	count, err := strconv.Atoi(line)
	if err != nil {
		return Value{}, err
	}

	if count == -1 {
		return Value{Type: Array, Null: true}, nil
	}
	if count < 0 {
		return Value{}, fmt.Errorf("invalid array length: %d", count)
	}
	if count > MaxArrayLen {
		return Value{}, fmt.Errorf("array length %d exceeds limit %d", count, MaxArrayLen)
	}

	array := make([]Value, count)
	for i := 0; i < count; i++ {
		val, err := r.Read()
		if err != nil {
			return Value{}, err
		}
		array[i] = val
	}

	return Value{Type: Array, Array: array}, nil
}

// Writer writes RESP values to an io.Writer
type Writer struct {
	writer *bufio.Writer
}

// NewWriter creates a new RESP writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: bufio.NewWriter(w)}
}

// Flush flushes the underlying writer
func (w *Writer) Flush() error {
	return w.writer.Flush()
}

// WriteValue writes a RESP value
func (w *Writer) WriteValue(v Value) error {
	var err error
	switch v.Type {
	case SimpleString:
		err = w.WriteSimpleString(v.Str)
	case Error:
		err = w.WriteError(v.Str)
	case Integer:
		err = w.WriteInteger(v.Num)
	case BulkString:
		if v.Null {
			err = w.WriteNull()
		} else {
			err = w.WriteBulkString(v.Bulk)
		}
	case Array:
		if v.Null {
			err = w.WriteNullArray()
		} else {
			err = w.WriteArray(v.Array)
		}
	case Map:
		err = w.WriteMap(v.MapData)
	case Push:
		err = w.WritePush(v.Array)
	default:
		err = fmt.Errorf("unknown RESP type: %c", v.Type)
	}
	return err
}

// WriteSimpleString writes a simple string
func (w *Writer) WriteSimpleString(s string) error {
	_, err := w.writer.WriteString("+" + s + "\r\n")
	return err
}

// WriteError writes an error
func (w *Writer) WriteError(s string) error {
	_, err := w.writer.WriteString("-" + s + "\r\n")
	return err
}

// WriteInteger writes an integer
func (w *Writer) WriteInteger(n int64) error {
	_, err := w.writer.WriteString(":" + strconv.FormatInt(n, 10) + "\r\n")
	return err
}

// WriteBulkString writes a bulk string
func (w *Writer) WriteBulkString(s string) error {
	_, err := w.writer.WriteString("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
	return err
}

// WriteNull writes a null bulk string
func (w *Writer) WriteNull() error {
	_, err := w.writer.WriteString("$-1\r\n")
	return err
}

// WriteNullArray writes a null array
func (w *Writer) WriteNullArray() error {
	_, err := w.writer.WriteString("*-1\r\n")
	return err
}

// WriteArray writes an array of values
func (w *Writer) WriteArray(arr []Value) error {
	if _, err := w.writer.WriteString("*" + strconv.Itoa(len(arr)) + "\r\n"); err != nil {
		return err
	}
	for _, v := range arr {
		if err := w.WriteValue(v); err != nil {
			return err
		}
	}
	return nil
}

// WritePush writes a RESP3 Push (out-of-band data) value
// Push is used for pub/sub messages in RESP3 - it's like an Array but with '>' prefix
func (w *Writer) WritePush(arr []Value) error {
	if _, err := w.writer.WriteString(">" + strconv.Itoa(len(arr)) + "\r\n"); err != nil {
		return err
	}
	for _, v := range arr {
		if err := w.WriteValue(v); err != nil {
			return err
		}
	}
	return nil
}

// WriteStringArray writes an array of strings as bulk strings
func (w *Writer) WriteStringArray(arr []string) error {
	if _, err := w.writer.WriteString("*" + strconv.Itoa(len(arr)) + "\r\n"); err != nil {
		return err
	}
	for _, s := range arr {
		if err := w.WriteBulkString(s); err != nil {
			return err
		}
	}
	return nil
}

// WriteMap writes a RESP3 Map (hash) value
func (w *Writer) WriteMap(m map[string]string) error {
	if _, err := w.writer.WriteString("%" + strconv.Itoa(len(m)) + "\r\n"); err != nil {
		return err
	}
	for key, value := range m {
		if err := w.WriteBulkString(key); err != nil {
			return err
		}
		if err := w.WriteBulkString(value); err != nil {
			return err
		}
	}
	return nil
}

// OK returns an OK simple string value
func OK() Value {
	return Value{Type: SimpleString, Str: "OK"}
}

// Err returns an error value with ERR prefix
func Err(msg string) Value {
	return Value{Type: Error, Str: "ERR " + msg}
}

// ErrCustom returns an error value with a custom prefix (no ERR prepended)
// Use for errors like NOSCRIPT, WRONGTYPE, MOVED, etc.
func ErrCustom(msg string) Value {
	return Value{Type: Error, Str: msg}
}

// ErrWrongType returns a wrong type error
func ErrWrongType() Value {
	return Value{Type: Error, Str: "WRONGTYPE Operation against a key holding the wrong kind of value"}
}

// ErrWrongArgs returns a wrong number of arguments error
func ErrWrongArgs(cmd string) Value {
	return Value{Type: Error, Str: fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd)}
}

// NullBulk returns a null bulk string value
func NullBulk() Value {
	return Value{Type: BulkString, Null: true}
}

// NullArray returns a null array value
func NullArray() Value {
	return Value{Type: Array, Null: true}
}

// Int returns an integer value
func Int(n int64) Value {
	return Value{Type: Integer, Num: n}
}

// Bulk returns a bulk string value
func Bulk(s string) Value {
	return Value{Type: BulkString, Bulk: s}
}

// Arr returns an array value
func Arr(values ...Value) Value {
	return Value{Type: Array, Array: values}
}

// MapVal returns a RESP3 Map value
func MapVal(m map[string]string) Value {
	return Value{Type: Map, MapData: m}
}
