package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

const cjsonMaxDepth = 1000

// openCJSON captures a per-state null sentinel so reassignment of cjson.null
// cannot change how the encoder and decoder recognize JSON null.
func openCJSON(L *lua.LState) {
	null := L.NewUserData()
	lib := L.NewTable()
	L.SetField(lib, "null", null)
	L.SetField(lib, "decode", L.NewFunction(func(L *lua.LState) int {
		if L.GetTop() != 1 {
			L.RaiseError("cjson.decode expects one argument")
			return 0
		}
		d := json.NewDecoder(strings.NewReader(L.CheckString(1)))
		d.UseNumber()
		value, err := decodeJSON(L, d, null, 0)
		if err == nil {
			var token any
			token, err = d.Token()
			if err == io.EOF {
				err = nil
			} else if err == nil {
				err = fmt.Errorf("unexpected trailing value %v", token)
			}
		}
		if err != nil {
			L.RaiseError("cjson.decode: %v", err)
			return 0
		}
		L.Push(value)
		return 1
	}))
	L.SetField(lib, "encode", L.NewFunction(func(L *lua.LState) int {
		if L.GetTop() != 1 {
			L.RaiseError("cjson.encode expects one argument")
			return 0
		}
		var b bytes.Buffer
		err := encodeJSON(&b, L.Get(1), null, make(map[*lua.LTable]bool), 0)
		if err != nil {
			L.RaiseError("cjson.encode: %v", err)
			return 0
		}
		L.Push(lua.LString(b.String()))
		return 1
	}))
	L.SetGlobal("cjson", lib)
}

func decodeJSON(L *lua.LState, d *json.Decoder, null *lua.LUserData, depth int) (lua.LValue, error) {
	tok, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch v := tok.(type) {
	case nil:
		return null, nil
	case bool:
		return lua.LBool(v), nil
	case string:
		return lua.LString(v), nil
	case json.Number:
		n, err := strconv.ParseFloat(string(v), 64)
		if err != nil || math.IsInf(n, 0) {
			return nil, fmt.Errorf("invalid number %s", v)
		}
		return lua.LNumber(n), nil
	case json.Delim:
		if v != '[' && v != '{' {
			return nil, fmt.Errorf("unexpected delimiter %c", v)
		}
		if depth >= cjsonMaxDepth {
			return nil, fmt.Errorf("maximum depth exceeded")
		}
		t := L.NewTable()
		i := 1
		for d.More() {
			var key string
			if v == '{' {
				k, err := d.Token()
				if err != nil {
					return nil, err
				}
				var ok bool
				key, ok = k.(string)
				if !ok {
					return nil, fmt.Errorf("invalid object key")
				}
			}
			child, err := decodeJSON(L, d, null, depth+1)
			if err != nil {
				return nil, err
			}
			if v == '[' {
				t.RawSetInt(i, child)
				i++
			} else {
				t.RawSetString(key, child)
			}
		}
		end, err := d.Token()
		if err != nil {
			return nil, err
		}
		expected := json.Delim(']')
		if v == '{' {
			expected = '}'
		}
		if end != expected {
			return nil, fmt.Errorf("invalid closing delimiter")
		}
		return t, nil
	}
	return nil, fmt.Errorf("invalid JSON value")
}

// encodeJSON reads tables without metamethods. Positive integer keys form arrays;
// the sparse cap bounds the work needed to fill gaps before writing an array.
func encodeJSON(b *bytes.Buffer, value lua.LValue, null *lua.LUserData, active map[*lua.LTable]bool, depth int) error {
	switch v := value.(type) {
	case *lua.LNilType:
		b.WriteString("null")
	case *lua.LUserData:
		if v != null {
			return fmt.Errorf("unsupported userdata")
		}
		b.WriteString("null")
	case lua.LBool:
		if bool(v) {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case lua.LNumber:
		n := float64(v)
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return fmt.Errorf("invalid number")
		}
		b.WriteString(strconv.FormatFloat(n, 'g', 14, 64))
	case lua.LString:
		data, _ := json.Marshal(string(v))
		b.Write(data)
	case *lua.LTable:
		if depth >= cjsonMaxDepth {
			return fmt.Errorf("maximum depth exceeded")
		}
		if active[v] {
			return fmt.Errorf("circular reference")
		}
		active[v] = true
		defer delete(active, v)
		count, max := 0, float64(0)
		array := true
		var keyErr error
		v.ForEach(func(k, _ lua.LValue) {
			count++
			switch k := k.(type) {
			case lua.LNumber:
				n := float64(k)
				if n < 1 || math.IsNaN(n) || math.Trunc(n) != n {
					array = false
				} else if n > max {
					max = n
				}
			case lua.LString:
				array = false
			default:
				keyErr = fmt.Errorf("unsupported table key")
			}
		})
		if keyErr != nil {
			return keyErr
		}
		if array && count > 0 {
			if max > 10 && max > 2*float64(count) {
				return fmt.Errorf("excessively sparse array")
			}
			b.WriteByte('[')
			for i := 1; float64(i) <= max; i++ {
				if i > 1 {
					b.WriteByte(',')
				}
				if err := encodeJSON(b, v.RawGetInt(i), null, active, depth+1); err != nil {
					return err
				}
			}
			b.WriteByte(']')
			return nil
		}
		entries := make(map[string]lua.LValue, count)
		v.ForEach(func(k, val lua.LValue) {
			var key string
			switch k := k.(type) {
			case lua.LString:
				key = string(k)
			case lua.LNumber:
				if math.IsInf(float64(k), 0) || math.IsNaN(float64(k)) {
					keyErr = fmt.Errorf("invalid numeric object key")
					return
				}
				key = strconv.FormatFloat(float64(k), 'g', 14, 64)
			}
			if _, exists := entries[key]; exists {
				keyErr = fmt.Errorf("duplicate object key %q", key)
			}
			entries[key] = val
		})
		if keyErr != nil {
			return keyErr
		}
		keys := make([]string, 0, len(entries))
		for k := range entries {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			data, _ := json.Marshal(key)
			b.Write(data)
			b.WriteByte(':')
			if err := encodeJSON(b, entries[key], null, active, depth+1); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	default:
		return fmt.Errorf("unsupported value type %s", value.Type())
	}
	return nil
}
