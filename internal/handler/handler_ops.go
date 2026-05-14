// Package handler implements Redis command handlers.
// This file contains the unified command handlers that work with storage.Operations.
package handler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/mnorrsken/postkeys/internal/resp"
	"github.com/mnorrsken/postkeys/internal/storage"
)

// parseScoreBound parses a Redis score bound string (e.g., "-inf", "+inf", "1.5", "(1.5")
func parseScoreBound(s string) (float64, error) {
	if s == "-inf" {
		return math.Inf(-1), nil
	}
	if s == "+inf" || s == "inf" {
		return math.Inf(1), nil
	}
	// Handle exclusive bounds (e.g., "(1.5")
	exclusive := strings.HasPrefix(s, "(")
	if exclusive {
		s = s[1:]
	}
	score, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, errors.New("min or max is not a float")
	}
	if exclusive {
		// Approximate exclusivity with tiny offset
		score += 1e-9
	}
	return score, nil
}

// ============== Unified Command Handlers ==============
// These handlers work with storage.Operations interface, which is implemented
// by both Backend (h.store) and Transaction (tx). This eliminates duplication
// between regular and transaction command handlers.

// ============== String Commands ==============

func (h *Handler) getOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("get")
	}

	value, found, err := ops.Get(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) setOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("set")
	}

	key := args[0].Bulk
	value := args[1].Bulk
	var ttl time.Duration

	// Parse options (EX, PX, NX, XX, etc.)
	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "EX":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			secs, err := strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer")
			}
			ttl = time.Duration(secs) * time.Second
		case "PX":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			ms, err := strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer")
			}
			ttl = time.Duration(ms) * time.Millisecond
		case "EXAT":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			ts, err := strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer")
			}
			ttl = time.Until(time.Unix(ts, 0))
		case "PXAT":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			ts, err := strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer")
			}
			ttl = time.Until(time.UnixMilli(ts))
		case "NX":
			// Set only if not exists
			ok, err := ops.SetNX(ctx, key, value)
			if err != nil {
				return resp.Err(err.Error())
			}
			if !ok {
				return resp.NullBulk()
			}
			return resp.OK()
		case "XX":
			// Set only if exists
			_, found, err := ops.Get(ctx, key)
			if err != nil {
				return resp.Err(err.Error())
			}
			if !found {
				return resp.NullBulk()
			}
		case "KEEPTTL":
			// Keep existing TTL
			currentTTL, err := ops.TTL(ctx, key)
			if err != nil {
				return resp.Err(err.Error())
			}
			if currentTTL > 0 {
				ttl = time.Duration(currentTTL) * time.Second
			}
		case "GET":
			// Return old value
			oldValue, found, err := ops.Get(ctx, key)
			if err != nil {
				return resp.Err(err.Error())
			}
			if err := ops.Set(ctx, key, value, ttl); err != nil {
				return resp.Err(err.Error())
			}
			if !found {
				return resp.NullBulk()
			}
			return resp.Bulk(oldValue)
		case "IFEQ", "IFGT":
			// Not implemented, ignore
			continue
		}
	}

	if err := ops.Set(ctx, key, value, ttl); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) setnxOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("setnx")
	}

	set, err := ops.SetNX(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if set {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) setexOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("setex")
	}

	secs, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	if err := ops.Set(ctx, args[0].Bulk, args[2].Bulk, time.Duration(secs)*time.Second); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) mgetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("mget")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	values, err := ops.MGet(ctx, keys)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(values))
	for i, val := range values {
		if val == nil {
			result[i] = resp.NullBulk()
		} else {
			result[i] = resp.Bulk(val.(string))
		}
	}
	return resp.Arr(result...)
}

func (h *Handler) msetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return resp.ErrWrongArgs("mset")
	}

	pairs := make(map[string]string)
	for i := 0; i < len(args); i += 2 {
		pairs[args[i].Bulk] = args[i+1].Bulk
	}

	if err := ops.MSet(ctx, pairs); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) incrOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("incr")
	}

	val, err := ops.Incr(ctx, args[0].Bulk, 1)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(val)
}

func (h *Handler) decrOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("decr")
	}

	val, err := ops.Incr(ctx, args[0].Bulk, -1)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(val)
}

func (h *Handler) incrbyOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("incrby")
	}

	delta, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	val, err := ops.Incr(ctx, args[0].Bulk, delta)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(val)
}

func (h *Handler) decrbyOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("decrby")
	}

	delta, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	val, err := ops.Incr(ctx, args[0].Bulk, -delta)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(val)
}

func (h *Handler) appendCmdOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("append")
	}

	length, err := ops.Append(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) getrangeOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("getrange")
	}

	key := args[0].Bulk
	start, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	end, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}

	result, err := ops.GetRange(ctx, key, start, end)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Bulk(result)
}

func (h *Handler) setrangeOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("setrange")
	}

	key := args[0].Bulk
	offset, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	if offset < 0 {
		return resp.Err("ERR offset is out of range")
	}
	value := args[2].Bulk

	length, err := ops.SetRange(ctx, key, offset, value)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) bitfieldOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("bitfield")
	}

	key := args[0].Bulk
	var bitfieldOps []storage.BitFieldOp

	i := 1
	for i < len(args) {
		opType := strings.ToUpper(args[i].Bulk)
		i++

		switch opType {
		case "GET":
			if i+2 > len(args) {
				return resp.Err("ERR syntax error")
			}
			encoding := args[i].Bulk
			offset, err := parseBitfieldOffset(args[i+1].Bulk, encoding)
			if err != nil {
				return resp.Err(err.Error())
			}
			bitfieldOps = append(bitfieldOps, storage.BitFieldOp{
				OpType:   "GET",
				Encoding: encoding,
				Offset:   offset,
			})
			i += 2

		case "SET":
			if i+3 > len(args) {
				return resp.Err("ERR syntax error")
			}
			encoding := args[i].Bulk
			offset, err := parseBitfieldOffset(args[i+1].Bulk, encoding)
			if err != nil {
				return resp.Err(err.Error())
			}
			value, err := strconv.ParseInt(args[i+2].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			bitfieldOps = append(bitfieldOps, storage.BitFieldOp{
				OpType:   "SET",
				Encoding: encoding,
				Offset:   offset,
				Value:    value,
			})
			i += 3

		case "INCRBY":
			if i+3 > len(args) {
				return resp.Err("ERR syntax error")
			}
			encoding := args[i].Bulk
			offset, err := parseBitfieldOffset(args[i+1].Bulk, encoding)
			if err != nil {
				return resp.Err(err.Error())
			}
			increment, err := strconv.ParseInt(args[i+2].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			bitfieldOps = append(bitfieldOps, storage.BitFieldOp{
				OpType:   "INCRBY",
				Encoding: encoding,
				Offset:   offset,
				Value:    increment,
			})
			i += 3

		case "OVERFLOW":
			// Skip overflow mode for now (default WRAP)
			if i < len(args) {
				i++ // skip the mode (WRAP, SAT, FAIL)
			}

		default:
			return resp.Err("ERR syntax error")
		}
	}

	results, err := ops.BitField(ctx, key, bitfieldOps)
	if err != nil {
		return resp.Err(err.Error())
	}

	// Return array of results
	values := make([]resp.Value, len(results))
	for i, r := range results {
		values[i] = resp.Int(r)
	}
	return resp.Value{Type: resp.Array, Array: values}
}

// parseBitfieldOffset parses a bitfield offset, handling # prefix for type-width multiplier
func parseBitfieldOffset(offsetStr, encoding string) (int64, error) {
	if len(offsetStr) > 0 && offsetStr[0] == '#' {
		// Type-width multiplier: #N means N * bitWidth
		multiplier, err := strconv.ParseInt(offsetStr[1:], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR bit offset is not an integer or out of range")
		}
		// Parse bit width from encoding
		bitWidth := int64(8)
		if len(encoding) > 1 {
			if bw, err := strconv.ParseInt(encoding[1:], 10, 64); err == nil {
				bitWidth = bw
			}
		}
		return multiplier * bitWidth, nil
	}
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR bit offset is not an integer or out of range")
	}
	return offset, nil
}

func (h *Handler) strlenOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("strlen")
	}

	length, err := ops.StrLen(ctx, args[0].Bulk)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) getexOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("getex")
	}

	key := args[0].Bulk
	var ttl time.Duration
	persist := false

	// Parse options: EX, PX, EXAT, PXAT, PERSIST
	for i := 1; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "EX":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			secs, err := strconv.ParseInt(args[i+1].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			ttl = time.Duration(secs) * time.Second
			i++
		case "PX":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			millis, err := strconv.ParseInt(args[i+1].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			ttl = time.Duration(millis) * time.Millisecond
			i++
		case "EXAT":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			ts, err := strconv.ParseInt(args[i+1].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			ttl = time.Until(time.Unix(ts, 0))
			i++
		case "PXAT":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			ts, err := strconv.ParseInt(args[i+1].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			ttl = time.Until(time.UnixMilli(ts))
			i++
		case "PERSIST":
			persist = true
		default:
			return resp.Err("ERR syntax error")
		}
	}

	value, exists, err := ops.GetEx(ctx, key, ttl, persist)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if !exists {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) getdelOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("getdel")
	}

	value, exists, err := ops.GetDel(ctx, args[0].Bulk)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if !exists {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) getsetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("getset")
	}

	oldValue, exists, err := ops.GetSet(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if !exists {
		return resp.NullBulk()
	}
	return resp.Bulk(oldValue)
}

func (h *Handler) incrbyfloatOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("incrbyfloat")
	}

	delta, err := strconv.ParseFloat(args[1].Bulk, 64)
	if err != nil {
		return resp.Err("ERR value is not a valid float")
	}

	result, err := ops.IncrByFloat(ctx, args[0].Bulk, delta)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Bulk(strconv.FormatFloat(result, 'f', -1, 64))
}

// ============== Key Commands ==============

func (h *Handler) delOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("del")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	deleted, err := ops.Del(ctx, keys)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(deleted)
}

func (h *Handler) existsOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("exists")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	count, err := ops.Exists(ctx, keys)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

// parseExpireOptions reads the optional NX/XX/GT/LT trailing args used by
// EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT. Returns the parsed options or a RESP
// error value (which the caller forwards) when the flags are invalid or
// mutually exclusive flags are combined.
func parseExpireOptions(args []resp.Value) (storage.ExpireOptions, *resp.Value) {
	var opts storage.ExpireOptions
	for _, a := range args {
		switch strings.ToUpper(a.Bulk) {
		case "NX":
			opts.NX = true
		case "XX":
			opts.XX = true
		case "GT":
			opts.GT = true
		case "LT":
			opts.LT = true
		default:
			e := resp.Err("ERR Unsupported option " + a.Bulk)
			return opts, &e
		}
	}
	// Redis 7 rejects NX combined with any of XX/GT/LT and GT combined with LT.
	if opts.NX && (opts.XX || opts.GT || opts.LT) {
		e := resp.Err("ERR NX and XX, GT or LT options at the same time are not compatible")
		return opts, &e
	}
	if opts.GT && opts.LT {
		e := resp.Err("ERR GT and LT options at the same time are not compatible")
		return opts, &e
	}
	return opts, nil
}

func (h *Handler) expireOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("expire")
	}

	secs, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}
	opts, errVal := parseExpireOptions(args[2:])
	if errVal != nil {
		return *errVal
	}

	ok, err := ops.Expire(ctx, args[0].Bulk, time.Duration(secs)*time.Second, opts)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) pexpireOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("pexpire")
	}

	ms, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}
	opts, errVal := parseExpireOptions(args[2:])
	if errVal != nil {
		return *errVal
	}

	ok, err := ops.Expire(ctx, args[0].Bulk, time.Duration(ms)*time.Millisecond, opts)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) expireatOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("expireat")
	}

	timestamp, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	opts, errVal := parseExpireOptions(args[2:])
	if errVal != nil {
		return *errVal
	}

	expireTime := time.Unix(timestamp, 0)
	ok, err := ops.ExpireAt(ctx, args[0].Bulk, expireTime, opts)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) pexpireatOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("pexpireat")
	}

	timestamp, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	opts, errVal := parseExpireOptions(args[2:])
	if errVal != nil {
		return *errVal
	}

	// Convert milliseconds to time
	expireTime := time.Unix(timestamp/1000, (timestamp%1000)*1000000)
	ok, err := ops.ExpireAt(ctx, args[0].Bulk, expireTime, opts)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

// HRANDFIELD key [count [WITHVALUES]]
//
// Without count → single random field as a bulk (or nil bulk if hash empty).
// With count → array of fields (or [field,value,...] if WITHVALUES given).
func (h *Handler) hrandfieldOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 3 {
		return resp.ErrWrongArgs("hrandfield")
	}
	key := args[0].Bulk
	if len(args) == 1 {
		out, err := ops.HRandField(ctx, key, 1, false)
		if err != nil {
			if strings.Contains(err.Error(), "WRONGTYPE") {
				return resp.ErrWrongType()
			}
			return resp.Err(err.Error())
		}
		if len(out) == 0 {
			return resp.NullBulk()
		}
		return resp.Bulk(out[0])
	}
	count, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	withValues := false
	if len(args) == 3 {
		if strings.ToUpper(args[2].Bulk) != "WITHVALUES" {
			return resp.Err("ERR syntax error")
		}
		withValues = true
	}
	out, err := ops.HRandField(ctx, key, count, withValues)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	result := make([]resp.Value, len(out))
	for i, s := range out {
		result[i] = resp.Bulk(s)
	}
	return resp.Arr(result...)
}

// SRANDMEMBER key [count]
func (h *Handler) srandmemberOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return resp.ErrWrongArgs("srandmember")
	}
	key := args[0].Bulk
	if len(args) == 1 {
		out, err := ops.SRandMember(ctx, key, 1)
		if err != nil {
			if strings.Contains(err.Error(), "WRONGTYPE") {
				return resp.ErrWrongType()
			}
			return resp.Err(err.Error())
		}
		if len(out) == 0 {
			return resp.NullBulk()
		}
		return resp.Bulk(out[0])
	}
	count, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	out, err := ops.SRandMember(ctx, key, count)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	result := make([]resp.Value, len(out))
	for i, m := range out {
		result[i] = resp.Bulk(m)
	}
	return resp.Arr(result...)
}

// ZRANDMEMBER key [count [WITHSCORES]]
func (h *Handler) zrandmemberOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 3 {
		return resp.ErrWrongArgs("zrandmember")
	}
	key := args[0].Bulk
	if len(args) == 1 {
		out, err := ops.ZRandMember(ctx, key, 1)
		if err != nil {
			if strings.Contains(err.Error(), "WRONGTYPE") {
				return resp.ErrWrongType()
			}
			return resp.Err(err.Error())
		}
		if len(out) == 0 {
			return resp.NullBulk()
		}
		return resp.Bulk(out[0].Member)
	}
	count, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	withScores := false
	if len(args) == 3 {
		if strings.ToUpper(args[2].Bulk) != "WITHSCORES" {
			return resp.Err("ERR syntax error")
		}
		withScores = true
	}
	out, err := ops.ZRandMember(ctx, key, count)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if withScores {
		result := make([]resp.Value, 0, len(out)*2)
		for _, m := range out {
			result = append(result, resp.Bulk(m.Member))
			result = append(result, resp.Bulk(strconv.FormatFloat(m.Score, 'f', -1, 64)))
		}
		return resp.Arr(result...)
	}
	result := make([]resp.Value, len(out))
	for i, m := range out {
		result[i] = resp.Bulk(m.Member)
	}
	return resp.Arr(result...)
}

// objectOp implements a subset of the Redis OBJECT command. Only ENCODING is
// meaningful here — postkeys stores one canonical representation per type,
// so the values are synthetic and cosmetic. Clients that branch on encoding
// (for testing or admin tools) still get a recognizable answer instead of an
// error. FREQ / IDLETIME / REFCOUNT / HELP return appropriate errors since
// they have no equivalent semantics on this backend.
func (h *Handler) objectOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("object")
	}
	sub := strings.ToUpper(args[0].Bulk)
	switch sub {
	case "ENCODING":
		if len(args) != 2 {
			return resp.ErrWrongArgs("object encoding")
		}
		kt, err := ops.Type(ctx, args[1].Bulk)
		if err != nil {
			return resp.Err(err.Error())
		}
		var encoding string
		switch kt {
		case storage.TypeNone:
			return resp.Err("ERR no such key")
		case storage.TypeString:
			encoding = "raw"
		case storage.TypeHash:
			encoding = "listpack"
		case storage.TypeList:
			encoding = "quicklist"
		case storage.TypeSet:
			encoding = "listpack"
		case storage.TypeZSet:
			encoding = "skiplist"
		default:
			encoding = "raw"
		}
		return resp.Bulk(encoding)
	case "HELP":
		return resp.Arr(
			resp.Bulk("OBJECT <subcommand> [<arg> ...]. Subcommands are:"),
			resp.Bulk("ENCODING <key>"),
			resp.Bulk("    Return the kind of internal representation used to store the value at <key>."),
		)
	case "FREQ", "IDLETIME", "REFCOUNT":
		return resp.Err("ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between maxmemory policies at runtime LFU and LRU data will take some time to adjust.")
	default:
		return resp.Err(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'. Try OBJECT HELP.", strings.ToLower(args[0].Bulk)))
	}
}

func (h *Handler) randomKeyOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 0 {
		return resp.ErrWrongArgs("randomkey")
	}
	key, found, err := ops.RandomKey(ctx)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(key)
}

func (h *Handler) copyOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("copy")
	}

	source := args[0].Bulk
	destination := args[1].Bulk
	replace := false

	// Parse optional arguments
	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Bulk)
		if opt == "REPLACE" {
			replace = true
		}
		// DB option is ignored (single DB)
	}

	ok, err := ops.Copy(ctx, source, destination, replace)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) ttlOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("ttl")
	}

	ttl, err := ops.TTL(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(ttl)
}

func (h *Handler) pttlOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("pttl")
	}

	pttl, err := ops.PTTL(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(pttl)
}

func (h *Handler) persistOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("persist")
	}

	ok, err := ops.Persist(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) keysOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("keys")
	}

	keys, err := ops.Keys(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(keys))
	for i, key := range keys {
		result[i] = resp.Bulk(key)
	}
	return resp.Arr(result...)
}

func (h *Handler) typeCmdOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("type")
	}

	keyType, err := ops.Type(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Value{Type: resp.SimpleString, Str: string(keyType)}
}

func (h *Handler) renameOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("rename")
	}

	err := ops.Rename(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

// ============== Hash Commands ==============

func (h *Handler) hgetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("hget")
	}

	value, found, err := ops.HGet(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) hsetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return resp.ErrWrongArgs("hset")
	}

	key := args[0].Bulk
	fields := make(map[string]string)
	for i := 1; i < len(args); i += 2 {
		fields[args[i].Bulk] = args[i+1].Bulk
	}

	added, err := ops.HSet(ctx, key, fields)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(added)
}

func (h *Handler) hdelOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("hdel")
	}

	key := args[0].Bulk
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = args[i].Bulk
	}

	deleted, err := ops.HDel(ctx, key, fields)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(deleted)
}

func (h *Handler) hgetallOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hgetall")
	}

	fields, err := ops.HGetAll(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	// Use RESP3 Map type if client supports it, otherwise use flat array (RESP2)
	if UseRESP3(ctx) {
		return resp.MapVal(fields)
	}

	// RESP2: Return flat array [field1, value1, field2, value2, ...]
	result := make([]resp.Value, 0, len(fields)*2)
	for key, value := range fields {
		result = append(result, resp.Bulk(key), resp.Bulk(value))
	}
	return resp.Arr(result...)
}

func (h *Handler) hmgetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("hmget")
	}

	key := args[0].Bulk
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = args[i].Bulk
	}

	values, err := ops.HMGet(ctx, key, fields)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(values))
	for i, val := range values {
		if val == nil {
			result[i] = resp.NullBulk()
		} else {
			result[i] = resp.Bulk(val.(string))
		}
	}
	return resp.Arr(result...)
}

func (h *Handler) hmsetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return resp.ErrWrongArgs("hmset")
	}

	key := args[0].Bulk
	fields := make(map[string]string)
	for i := 1; i < len(args); i += 2 {
		fields[args[i].Bulk] = args[i+1].Bulk
	}

	_, err := ops.HSet(ctx, key, fields)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) hexistsOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("hexists")
	}

	exists, err := ops.HExists(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if exists {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) hkeysOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hkeys")
	}

	keys, err := ops.HKeys(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(keys))
	for i, key := range keys {
		result[i] = resp.Bulk(key)
	}
	return resp.Arr(result...)
}

func (h *Handler) hvalsOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hvals")
	}

	vals, err := ops.HVals(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(vals))
	for i, val := range vals {
		result[i] = resp.Bulk(val)
	}
	return resp.Arr(result...)
}

func (h *Handler) hlenOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hlen")
	}

	length, err := ops.HLen(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) hincrbyOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("hincrby")
	}

	key := args[0].Bulk
	field := args[1].Bulk
	increment, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}

	result, err := ops.HIncrBy(ctx, key, field, increment)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(result)
}

func (h *Handler) hincrbyfloatOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("hincrbyfloat")
	}

	key := args[0].Bulk
	field := args[1].Bulk
	increment, err := strconv.ParseFloat(args[2].Bulk, 64)
	if err != nil {
		return resp.Err("ERR value is not a valid float")
	}

	result, err := ops.HIncrByFloat(ctx, key, field, increment)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Bulk(strconv.FormatFloat(result, 'f', -1, 64))
}

func (h *Handler) hsetnxOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("hsetnx")
	}

	set, err := ops.HSetNX(ctx, args[0].Bulk, args[1].Bulk, args[2].Bulk)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if set {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) hscanOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("hscan")
	}

	key := args[0].Bulk
	// cursor is args[1], we ignore it since we return all results at once

	// Parse optional MATCH and COUNT arguments
	var pattern string
	for i := 2; i < len(args)-1; i += 2 {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "MATCH":
			pattern = args[i+1].Bulk
		case "COUNT":
			// Ignore COUNT, we return all matches
		}
	}

	// Get all fields from the hash
	fields, err := ops.HGetAll(ctx, key)
	if err != nil {
		return resp.Err(err.Error())
	}

	// Build result array with field-value pairs
	result := make([]resp.Value, 0, len(fields)*2)
	for field, value := range fields {
		// Apply pattern matching if specified
		if pattern != "" && pattern != "*" {
			matched, _ := matchGlob(pattern, field)
			if !matched {
				continue
			}
		}
		result = append(result, resp.Bulk(field), resp.Bulk(value))
	}

	// HSCAN returns [cursor, [field1, value1, field2, value2, ...]]
	// We always return cursor "0" to indicate scan is complete
	return resp.Arr(
		resp.Bulk("0"),
		resp.Arr(result...),
	)
}

// matchGlob performs simple glob pattern matching (supports * and ?)
func matchGlob(pattern, s string) (bool, error) {
	pi, si := 0, 0
	starIdx, matchIdx := -1, 0

	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]) {
			pi++
			si++
		} else if pi < len(pattern) && pattern[pi] == '*' {
			starIdx = pi
			matchIdx = si
			pi++
		} else if starIdx != -1 {
			pi = starIdx + 1
			matchIdx++
			si = matchIdx
		} else {
			return false, nil
		}
	}

	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}

	return pi == len(pattern), nil
}

// ============== Watch Commands ==============
// WATCH and UNWATCH are used for optimistic locking in Redis.
// Since we use PostgreSQL transactions with proper isolation,
// we implement these as no-ops for compatibility.

func (h *Handler) watchOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	// WATCH is a no-op - PostgreSQL transactions provide proper isolation
	return resp.OK()
}

func (h *Handler) unwatchOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	// UNWATCH is a no-op - PostgreSQL transactions provide proper isolation
	return resp.OK()
}

// ============== List Commands ==============

func (h *Handler) lpushOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("lpush")
	}

	key := args[0].Bulk
	values := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = args[i].Bulk
	}

	length, err := ops.LPush(ctx, key, values)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	// Notify any BRPOP/BLPOP waiters
	if h.listNotifier != nil {
		h.listNotifier.NotifyPush(ctx, key)
	}

	return resp.Int(length)
}

func (h *Handler) rpushOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("rpush")
	}

	key := args[0].Bulk
	values := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = args[i].Bulk
	}

	length, err := ops.RPush(ctx, key, values)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	// Notify any BRPOP/BLPOP waiters
	if h.listNotifier != nil {
		h.listNotifier.NotifyPush(ctx, key)
	}

	return resp.Int(length)
}

func (h *Handler) lpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("lpop")
	}

	value, found, err := ops.LPop(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) rpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("rpop")
	}

	value, found, err := ops.RPop(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) llenOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("llen")
	}

	length, err := ops.LLen(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) lrangeOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("lrange")
	}

	start, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	stop, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	values, err := ops.LRange(ctx, args[0].Bulk, start, stop)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(values))
	for i, val := range values {
		result[i] = resp.Bulk(val)
	}
	return resp.Arr(result...)
}

func (h *Handler) lindexOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("lindex")
	}

	index, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	value, found, err := ops.LIndex(ctx, args[0].Bulk, index)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

// brpopOp implements BRPOP - blocking right pop from list(s)
// BRPOP key [key ...] timeout
func (h *Handler) brpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("brpop")
	}

	// Last arg is timeout in seconds
	timeout, err := strconv.ParseFloat(args[len(args)-1].Bulk, 64)
	if err != nil {
		return resp.Err("timeout is not a float or out of range")
	}

	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = args[i].Bulk
	}

	return h.blockingPop(ctx, ops, keys, timeout, ops.RPopMulti)
}

// blpopOp implements BLPOP - blocking left pop from list(s)
// BLPOP key [key ...] timeout
func (h *Handler) blpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("blpop")
	}

	// Last arg is timeout in seconds
	timeout, err := strconv.ParseFloat(args[len(args)-1].Bulk, 64)
	if err != nil {
		return resp.Err("timeout is not a float or out of range")
	}

	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = args[i].Bulk
	}

	return h.blockingPop(ctx, ops, keys, timeout, ops.LPopMulti)
}

// popFn pops from the first non-empty key among keys and returns
// (poppedKey, value, found, err). Used to share BLPOP and BRPOP loops.
type popFn func(ctx context.Context, keys []string) (string, string, bool, error)

func (h *Handler) blockingPop(ctx context.Context, _ storage.Operations, keys []string, timeout float64, pop popFn) resp.Value {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(time.Duration(timeout * float64(time.Second)))
	}

	for {
		// One round-trip across all keys; SQL picks the first non-empty list.
		poppedKey, value, found, err := pop(ctx, keys)
		if err != nil {
			return resp.Err(err.Error())
		}
		if found {
			return resp.Arr(resp.Bulk(poppedKey), resp.Bulk(value))
		}

		if timeout > 0 && time.Now().After(deadline) {
			return resp.NullBulk()
		}

		waitTime := h.blockingPollInterval
		if timeout > 0 {
			remaining := time.Until(deadline)
			if remaining < waitTime {
				waitTime = remaining
			}
		}

		if h.listNotifier != nil {
			if h.listNotifier.WaitForKeys(ctx, keys, waitTime) != "" {
				continue
			}
		} else {
			select {
			case <-ctx.Done():
				return resp.NullBulk()
			case <-time.After(waitTime):
			}
		}

		select {
		case <-ctx.Done():
			return resp.NullBulk()
		default:
		}
	}
}

// parseMPopHead consumes the common prefix of LMPOP/BLMPOP/ZMPOP/BZMPOP:
// `numkeys key [key ...] WHERE [COUNT count]` where WHERE ∈ {LEFT,RIGHT,MIN,MAX}.
// It validates that whereStr is in allowedWhere and returns the parsed pieces.
func parseMPopHead(args []resp.Value, allowedWhere map[string]bool) (keys []string, where string, count int64, errVal *resp.Value) {
	count = 1
	if len(args) < 2 {
		e := resp.Err("ERR syntax error")
		return nil, "", 0, &e
	}
	numkeys, err := strconv.ParseInt(args[0].Bulk, 10, 64)
	if err != nil || numkeys < 1 {
		e := resp.Err("ERR numkeys should be greater than 0")
		return nil, "", 0, &e
	}
	if int64(len(args)) < 1+numkeys+1 {
		e := resp.Err("ERR syntax error")
		return nil, "", 0, &e
	}
	keys = make([]string, numkeys)
	for i := int64(0); i < numkeys; i++ {
		keys[i] = args[1+i].Bulk
	}
	where = strings.ToUpper(args[1+numkeys].Bulk)
	if !allowedWhere[where] {
		e := resp.Err("ERR syntax error")
		return nil, "", 0, &e
	}
	rest := args[2+numkeys:]
	if len(rest) > 0 {
		if len(rest) != 2 || strings.ToUpper(rest[0].Bulk) != "COUNT" {
			e := resp.Err("ERR syntax error")
			return nil, "", 0, &e
		}
		count, err = strconv.ParseInt(rest[1].Bulk, 10, 64)
		if err != nil || count < 1 {
			e := resp.Err("ERR count should be greater than 0")
			return nil, "", 0, &e
		}
	}
	return keys, where, count, nil
}

// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
func (h *Handler) lmpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	keys, where, count, errVal := parseMPopHead(args, map[string]bool{"LEFT": true, "RIGHT": true})
	if errVal != nil {
		return *errVal
	}
	return doLMPop(ctx, ops, keys, where, count)
}

func doLMPop(ctx context.Context, ops storage.Operations, keys []string, where string, count int64) resp.Value {
	var poppedKey string
	var values []string
	var found bool
	var err error
	if where == "LEFT" {
		poppedKey, values, found, err = ops.LMPop(ctx, keys, count)
	} else {
		poppedKey, values, found, err = ops.RMPop(ctx, keys, count)
	}
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullArray()
	}
	inner := make([]resp.Value, len(values))
	for i, v := range values {
		inner[i] = resp.Bulk(v)
	}
	return resp.Arr(resp.Bulk(poppedKey), resp.Arr(inner...))
}

// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
func (h *Handler) blmpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 4 {
		return resp.ErrWrongArgs("blmpop")
	}
	timeout, err := strconv.ParseFloat(args[0].Bulk, 64)
	if err != nil {
		return resp.Err("timeout is not a float or out of range")
	}
	keys, where, count, errVal := parseMPopHead(args[1:], map[string]bool{"LEFT": true, "RIGHT": true})
	if errVal != nil {
		return *errVal
	}
	return h.blockingMPop(ctx, keys, timeout, func() resp.Value {
		return doLMPop(ctx, ops, keys, where, count)
	})
}

// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
func (h *Handler) zmpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	keys, where, count, errVal := parseMPopHead(args, map[string]bool{"MIN": true, "MAX": true})
	if errVal != nil {
		return *errVal
	}
	return doZMPop(ctx, ops, keys, where, count)
}

func doZMPop(ctx context.Context, ops storage.Operations, keys []string, where string, count int64) resp.Value {
	var poppedKey string
	var members []storage.ZMember
	var found bool
	var err error
	if where == "MIN" {
		poppedKey, members, found, err = ops.ZMPopMin(ctx, keys, count)
	} else {
		poppedKey, members, found, err = ops.ZMPopMax(ctx, keys, count)
	}
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullArray()
	}
	inner := make([]resp.Value, len(members))
	for i, m := range members {
		inner[i] = resp.Arr(resp.Bulk(m.Member), resp.Bulk(strconv.FormatFloat(m.Score, 'f', -1, 64)))
	}
	return resp.Arr(resp.Bulk(poppedKey), resp.Arr(inner...))
}

// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
// Unlike BLPOP/BRPOP, blocking zset pops have no LISTEN/NOTIFY equivalent
// so they always use the poll fallback.
func (h *Handler) bzmpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 4 {
		return resp.ErrWrongArgs("bzmpop")
	}
	timeout, err := strconv.ParseFloat(args[0].Bulk, 64)
	if err != nil {
		return resp.Err("timeout is not a float or out of range")
	}
	keys, where, count, errVal := parseMPopHead(args[1:], map[string]bool{"MIN": true, "MAX": true})
	if errVal != nil {
		return *errVal
	}
	return h.blockingMPop(ctx, keys, timeout, func() resp.Value {
		return doZMPop(ctx, ops, keys, where, count)
	})
}

// blockingMPop loops popping until popOnce returns a non-null result, the
// context is cancelled, or the deadline expires. popOnce returns a resp.Value
// already shaped for the client; a null array is treated as "nothing yet".
func (h *Handler) blockingMPop(ctx context.Context, keys []string, timeout float64, popOnce func() resp.Value) resp.Value {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(time.Duration(timeout * float64(time.Second)))
	}
	for {
		v := popOnce()
		// Errors short-circuit immediately.
		if v.Type == resp.Error {
			return v
		}
		// A null array means "nothing in any key right now". Any other
		// response — including a real (non-null) array — means we succeeded.
		if !(v.Type == resp.Array && v.Null) {
			return v
		}

		if timeout > 0 && time.Now().After(deadline) {
			return resp.NullArray()
		}
		waitTime := h.blockingPollInterval
		if timeout > 0 {
			remaining := time.Until(deadline)
			if remaining < waitTime {
				waitTime = remaining
			}
		}
		if h.listNotifier != nil {
			if h.listNotifier.WaitForKeys(ctx, keys, waitTime) != "" {
				continue
			}
		} else {
			select {
			case <-ctx.Done():
				return resp.NullArray()
			case <-time.After(waitTime):
			}
		}
		select {
		case <-ctx.Done():
			return resp.NullArray()
		default:
		}
	}
}

// ============== Key Scan Commands ==============

// scanOp implements SCAN - incrementally iterate over keys
// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
func (h *Handler) scanOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("scan")
	}

	cursor, err := strconv.ParseInt(args[0].Bulk, 10, 64)
	if err != nil {
		return resp.Err("invalid cursor")
	}

	// Parse optional arguments
	pattern := "*"
	count := int64(10)
	var typeFilter string

	for i := 1; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "MATCH":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			pattern = args[i].Bulk
		case "COUNT":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			count, err = strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer or out of range")
			}
		case "TYPE":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			typeFilter = strings.ToLower(args[i].Bulk)
		}
	}

	// Get all matching keys
	allKeys, err := ops.Keys(ctx, pattern)
	if err != nil {
		return resp.Err(err.Error())
	}

	// Filter by type if specified
	var filteredKeys []string
	if typeFilter != "" {
		for _, key := range allKeys {
			keyType, err := ops.Type(ctx, key)
			if err != nil {
				continue
			}
			if string(keyType) == typeFilter {
				filteredKeys = append(filteredKeys, key)
			}
		}
	} else {
		filteredKeys = allKeys
	}

	// Simulate cursor-based pagination
	// cursor is the start index, we return up to 'count' keys
	start := int(cursor)
	if start >= len(filteredKeys) {
		// No more keys, return cursor 0 (end of iteration)
		return resp.Arr(resp.Bulk("0"), resp.Arr())
	}

	end := start + int(count)
	if end > len(filteredKeys) {
		end = len(filteredKeys)
	}

	resultKeys := filteredKeys[start:end]

	// Calculate next cursor
	var nextCursor string
	if end >= len(filteredKeys) {
		nextCursor = "0" // End of iteration
	} else {
		nextCursor = strconv.Itoa(end)
	}

	// Build result array
	keyValues := make([]resp.Value, len(resultKeys))
	for i, key := range resultKeys {
		keyValues[i] = resp.Bulk(key)
	}

	return resp.Arr(resp.Bulk(nextCursor), resp.Arr(keyValues...))
}

// ============== Set Commands ==============

func (h *Handler) saddOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("sadd")
	}

	key := args[0].Bulk
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = args[i].Bulk
	}

	added, err := ops.SAdd(ctx, key, members)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(added)
}

func (h *Handler) sremOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("srem")
	}

	key := args[0].Bulk
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = args[i].Bulk
	}

	removed, err := ops.SRem(ctx, key, members)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(removed)
}

func (h *Handler) smembersOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("smembers")
	}

	members, err := ops.SMembers(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(members))
	for i, member := range members {
		result[i] = resp.Bulk(member)
	}
	return resp.Arr(result...)
}

func (h *Handler) sismemberOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("sismember")
	}

	exists, err := ops.SIsMember(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if exists {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) scardOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("scard")
	}

	count, err := ops.SCard(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

// ============== Sorted Set Commands ==============

func (h *Handler) zaddOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("zadd")
	}

	key := args[0].Bulk

	// Parse optional flags (NX, XX, GT, LT, CH)
	i := 1
	var nx, xx, gt, lt, ch bool

	for i < len(args) {
		flag := strings.ToUpper(args[i].Bulk)
		switch flag {
		case "NX":
			nx = true
			i++
		case "XX":
			xx = true
			i++
		case "GT":
			gt = true
			i++
		case "LT":
			lt = true
			i++
		case "CH":
			ch = true
			i++
		default:
			// Not a flag, must be score-member pairs
			goto parseMembers
		}
	}

parseMembers:
	// NX and XX are mutually exclusive
	if nx && xx {
		return resp.Err("ERR XX and NX options at the same time are not compatible")
	}

	// GT and LT are mutually exclusive
	if gt && lt {
		return resp.Err("ERR GT and LT options at the same time are not compatible")
	}

	// GT/LT require XX or no NX
	if (gt || lt) && nx {
		return resp.Err("ERR GT/LT and NX options at the same time are not compatible")
	}

	// Check if we have score-member pairs
	remaining := len(args) - i
	if remaining == 0 || remaining%2 != 0 {
		return resp.ErrWrongArgs("zadd")
	}

	var members []storage.ZMember
	for i < len(args) {
		score, err := strconv.ParseFloat(args[i].Bulk, 64)
		if err != nil {
			return resp.Err("ERR value is not a valid float")
		}
		member := args[i+1].Bulk
		members = append(members, storage.ZMember{Member: member, Score: score})
		i += 2
	}

	// For now, we ignore NX/XX/GT/LT/CH flags and do basic ZADD
	// TODO: implement full flag support in storage layer
	_ = nx
	_ = xx
	_ = gt
	_ = lt
	_ = ch

	added, err := ops.ZAdd(ctx, key, members)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(added)
}

func (h *Handler) zrangeOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("zrange")
	}

	key := args[0].Bulk
	startArg := args[1].Bulk
	stopArg := args[2].Bulk

	// Parse optional modifiers
	withScores := false
	byScore := false
	rev := false
	var offset, count int64 = 0, -1

	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i].Bulk) {
		case "WITHSCORES":
			withScores = true
		case "BYSCORE":
			byScore = true
		case "BYLEX":
			// BYLEX not fully supported, treat as BYSCORE for now
			return resp.Err("ERR BYLEX not supported")
		case "REV":
			rev = true
		case "LIMIT":
			if i+2 >= len(args) {
				return resp.ErrWrongArgs("zrange")
			}
			var err error
			offset, err = strconv.ParseInt(args[i+1].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer or out of range")
			}
			count, err = strconv.ParseInt(args[i+2].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer or out of range")
			}
			i += 2
		}
	}

	// Handle BYSCORE mode (Redis 6.2+ unified ZRANGE)
	if byScore {
		min, err := parseScoreBound(startArg)
		if err != nil {
			return resp.Err("min or max is not a float")
		}
		max, err := parseScoreBound(stopArg)
		if err != nil {
			return resp.Err("min or max is not a float")
		}

		if rev {
			// Swap min/max for REV
			min, max = max, min
		}

		members, err := ops.ZRangeByScore(ctx, key, min, max, withScores, offset, count)
		if err != nil {
			if strings.Contains(err.Error(), "WRONGTYPE") {
				return resp.ErrWrongType()
			}
			return resp.Err(err.Error())
		}

		if rev {
			// Reverse the result for REV
			for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
				members[i], members[j] = members[j], members[i]
			}
		}

		if withScores {
			result := make([]resp.Value, 0, len(members)*2)
			for _, m := range members {
				result = append(result, resp.Bulk(m.Member))
				result = append(result, resp.Bulk(strconv.FormatFloat(m.Score, 'f', -1, 64)))
			}
			return resp.Arr(result...)
		}

		result := make([]resp.Value, len(members))
		for i, m := range members {
			result[i] = resp.Bulk(m.Member)
		}
		return resp.Arr(result...)
	}

	// Standard index-based ZRANGE
	start, err := strconv.ParseInt(startArg, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(stopArg, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer or out of range")
	}

	members, err := ops.ZRange(ctx, key, start, stop, withScores)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	if rev {
		// Reverse the result for REV
		for i, j := 0, len(members)-1; i < j; i, j = i+1, j-1 {
			members[i], members[j] = members[j], members[i]
		}
	}

	if withScores {
		result := make([]resp.Value, 0, len(members)*2)
		for _, m := range members {
			result = append(result, resp.Bulk(m.Member))
			result = append(result, resp.Bulk(strconv.FormatFloat(m.Score, 'f', -1, 64)))
		}
		return resp.Arr(result...)
	}

	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.Bulk(m.Member)
	}
	return resp.Arr(result...)
}

// ZREVRANGE key start stop [WITHSCORES]
func (h *Handler) zrevrangeOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("zrevrange")
	}
	start, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer or out of range")
	}
	withScores := false
	for _, a := range args[3:] {
		if strings.ToUpper(a.Bulk) == "WITHSCORES" {
			withScores = true
		} else {
			return resp.Err("ERR syntax error")
		}
	}

	members, err := ops.ZRevRange(ctx, args[0].Bulk, start, stop, withScores)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if withScores {
		result := make([]resp.Value, 0, len(members)*2)
		for _, m := range members {
			result = append(result, resp.Bulk(m.Member))
			result = append(result, resp.Bulk(strconv.FormatFloat(m.Score, 'f', -1, 64)))
		}
		return resp.Arr(result...)
	}
	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.Bulk(m.Member)
	}
	return resp.Arr(result...)
}

// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
// Note: max is the first arg, min the second (Redis quirk).
func (h *Handler) zrevrangebyscoreOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("zrevrangebyscore")
	}
	// Wire order is max, min — flip to canonical min, max for the storage call.
	max, err := parseScoreBound(args[1].Bulk)
	if err != nil {
		return resp.Err("min or max is not a float")
	}
	min, err := parseScoreBound(args[2].Bulk)
	if err != nil {
		return resp.Err("min or max is not a float")
	}

	withScores := false
	var offset, count int64 = 0, -1
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i].Bulk) {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return resp.ErrWrongArgs("zrevrangebyscore")
			}
			offset, err = strconv.ParseInt(args[i+1].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			count, err = strconv.ParseInt(args[i+2].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			i += 2
		default:
			return resp.Err("ERR syntax error")
		}
	}

	members, err := ops.ZRevRangeByScore(ctx, args[0].Bulk, min, max, withScores, offset, count)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if withScores {
		result := make([]resp.Value, 0, len(members)*2)
		for _, m := range members {
			result = append(result, resp.Bulk(m.Member))
			result = append(result, resp.Bulk(strconv.FormatFloat(m.Score, 'f', -1, 64)))
		}
		return resp.Arr(result...)
	}
	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.Bulk(m.Member)
	}
	return resp.Arr(result...)
}

// parseLexBound parses Redis lex-range tokens: "-", "+", "[member", "(member".
func parseLexBound(s string) (storage.LexBound, error) {
	switch {
	case s == "-":
		return storage.LexBound{Infinity: -1}, nil
	case s == "+":
		return storage.LexBound{Infinity: +1}, nil
	case strings.HasPrefix(s, "["):
		return storage.LexBound{Value: s[1:], Inclusive: true}, nil
	case strings.HasPrefix(s, "("):
		return storage.LexBound{Value: s[1:], Inclusive: false}, nil
	default:
		return storage.LexBound{}, fmt.Errorf("min or max not valid string range item")
	}
}

// parseLexLimit walks optional [LIMIT offset count] trailing args.
func parseLexLimit(args []resp.Value) (offset, count int64, errVal *resp.Value) {
	if len(args) == 0 {
		return 0, -1, nil
	}
	if len(args) != 3 || strings.ToUpper(args[0].Bulk) != "LIMIT" {
		e := resp.Err("ERR syntax error")
		return 0, 0, &e
	}
	o, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		e := resp.Err("ERR value is not an integer or out of range")
		return 0, 0, &e
	}
	c, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil {
		e := resp.Err("ERR value is not an integer or out of range")
		return 0, 0, &e
	}
	return o, c, nil
}

// ZRANGEBYLEX key min max [LIMIT offset count]
func (h *Handler) zrangebylexOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("zrangebylex")
	}
	min, err := parseLexBound(args[1].Bulk)
	if err != nil {
		return resp.Err("ERR " + err.Error())
	}
	max, err := parseLexBound(args[2].Bulk)
	if err != nil {
		return resp.Err("ERR " + err.Error())
	}
	offset, count, errVal := parseLexLimit(args[3:])
	if errVal != nil {
		return *errVal
	}
	members, err := ops.ZRangeByLex(ctx, args[0].Bulk, min, max, offset, count)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.Bulk(m)
	}
	return resp.Arr(result...)
}

// ZREVRANGEBYLEX key max min [LIMIT offset count]
// Note the wire-protocol order: max first, then min.
func (h *Handler) zrevrangebylexOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("zrevrangebylex")
	}
	max, err := parseLexBound(args[1].Bulk)
	if err != nil {
		return resp.Err("ERR " + err.Error())
	}
	min, err := parseLexBound(args[2].Bulk)
	if err != nil {
		return resp.Err("ERR " + err.Error())
	}
	offset, count, errVal := parseLexLimit(args[3:])
	if errVal != nil {
		return *errVal
	}
	members, err := ops.ZRevRangeByLex(ctx, args[0].Bulk, min, max, offset, count)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.Bulk(m)
	}
	return resp.Arr(result...)
}

// ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
func (h *Handler) zrangestoreOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 4 {
		return resp.ErrWrongArgs("zrangestore")
	}
	dst := args[0].Bulk
	src := args[1].Bulk
	startArg := args[2].Bulk
	stopArg := args[3].Bulk

	spec := storage.ZRangeStoreSpec{By: storage.ZRangeByIndex, Count: -1}

	for i := 4; i < len(args); i++ {
		switch strings.ToUpper(args[i].Bulk) {
		case "BYSCORE":
			spec.By = storage.ZRangeByScore
		case "BYLEX":
			spec.By = storage.ZRangeByLex
		case "REV":
			spec.Rev = true
		case "LIMIT":
			if i+2 >= len(args) {
				return resp.ErrWrongArgs("zrangestore")
			}
			off, err := strconv.ParseInt(args[i+1].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			cnt, err := strconv.ParseInt(args[i+2].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			spec.Offset, spec.Count = off, cnt
			i += 2
		default:
			return resp.Err("ERR syntax error")
		}
	}

	switch spec.By {
	case storage.ZRangeByIndex:
		start, err := strconv.ParseInt(startArg, 10, 64)
		if err != nil {
			return resp.Err("ERR value is not an integer or out of range")
		}
		stop, err := strconv.ParseInt(stopArg, 10, 64)
		if err != nil {
			return resp.Err("ERR value is not an integer or out of range")
		}
		spec.Start, spec.Stop = start, stop
	case storage.ZRangeByScore:
		min, err := parseScoreBound(startArg)
		if err != nil {
			return resp.Err("min or max is not a float")
		}
		max, err := parseScoreBound(stopArg)
		if err != nil {
			return resp.Err("min or max is not a float")
		}
		if spec.Rev {
			min, max = max, min
		}
		spec.MinScore, spec.MaxScore = min, max
	case storage.ZRangeByLex:
		min, err := parseLexBound(startArg)
		if err != nil {
			return resp.Err("ERR " + err.Error())
		}
		max, err := parseLexBound(stopArg)
		if err != nil {
			return resp.Err("ERR " + err.Error())
		}
		if spec.Rev {
			min, max = max, min
		}
		spec.MinLex, spec.MaxLex = min, max
	}

	n, err := ops.ZRangeStore(ctx, dst, src, spec)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(n)
}

// ZLEXCOUNT key min max
func (h *Handler) zlexcountOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("zlexcount")
	}
	min, err := parseLexBound(args[1].Bulk)
	if err != nil {
		return resp.Err("ERR " + err.Error())
	}
	max, err := parseLexBound(args[2].Bulk)
	if err != nil {
		return resp.Err("ERR " + err.Error())
	}
	n, err := ops.ZLexCount(ctx, args[0].Bulk, min, max)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(n)
}

func (h *Handler) zscoreOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("zscore")
	}

	score, found, err := ops.ZScore(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(strconv.FormatFloat(score, 'f', -1, 64))
}

func (h *Handler) zremOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("zrem")
	}

	key := args[0].Bulk
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = args[i].Bulk
	}

	removed, err := ops.ZRem(ctx, key, members)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(removed)
}

func (h *Handler) zcardOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("zcard")
	}

	count, err := ops.ZCard(ctx, args[0].Bulk)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) zrangebyscoreOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("zrangebyscore")
	}

	key := args[0].Bulk
	minStr := args[1].Bulk
	maxStr := args[2].Bulk

	// Parse min score
	var min float64
	if minStr == "-inf" {
		min = -1e308
	} else if minStr == "+inf" || minStr == "inf" {
		min = 1e308
	} else {
		// Handle exclusive bounds (e.g., "(1.5")
		exclusive := strings.HasPrefix(minStr, "(")
		if exclusive {
			minStr = minStr[1:]
		}
		var err error
		min, err = strconv.ParseFloat(minStr, 64)
		if err != nil {
			return resp.Err("ERR min value is not a float")
		}
		if exclusive {
			min += 1e-9 // Approximate exclusivity
		}
	}

	// Parse max score
	var max float64
	if maxStr == "+inf" || maxStr == "inf" {
		max = 1e308
	} else if maxStr == "-inf" {
		max = -1e308
	} else {
		exclusive := strings.HasPrefix(maxStr, "(")
		if exclusive {
			maxStr = maxStr[1:]
		}
		var err error
		max, err = strconv.ParseFloat(maxStr, 64)
		if err != nil {
			return resp.Err("ERR max value is not a float")
		}
		if exclusive {
			max -= 1e-9
		}
	}

	// Parse optional arguments
	withScores := false
	var offset, count int64 = 0, -1

	for i := 3; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return resp.ErrWrongArgs("zrangebyscore")
			}
			var err error
			offset, err = strconv.ParseInt(args[i+1].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			count, err = strconv.ParseInt(args[i+2].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			i += 2
		}
	}

	members, err := ops.ZRangeByScore(ctx, key, min, max, withScores, offset, count)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	if withScores {
		result := make([]resp.Value, 0, len(members)*2)
		for _, m := range members {
			result = append(result, resp.Bulk(m.Member))
			result = append(result, resp.Bulk(strconv.FormatFloat(m.Score, 'f', -1, 64)))
		}
		return resp.Arr(result...)
	}

	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.Bulk(m.Member)
	}
	return resp.Arr(result...)
}

func (h *Handler) zremrangebyscoreOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("zremrangebyscore")
	}

	key := args[0].Bulk
	minStr := args[1].Bulk
	maxStr := args[2].Bulk

	// Parse min score
	var min float64
	if minStr == "-inf" {
		min = -1e308
	} else if minStr == "+inf" || minStr == "inf" {
		min = 1e308
	} else {
		exclusive := strings.HasPrefix(minStr, "(")
		if exclusive {
			minStr = minStr[1:]
		}
		var err error
		min, err = strconv.ParseFloat(minStr, 64)
		if err != nil {
			return resp.Err("ERR min value is not a float")
		}
		if exclusive {
			min += 1e-9
		}
	}

	// Parse max score
	var max float64
	if maxStr == "+inf" || maxStr == "inf" {
		max = 1e308
	} else if maxStr == "-inf" {
		max = -1e308
	} else {
		exclusive := strings.HasPrefix(maxStr, "(")
		if exclusive {
			maxStr = maxStr[1:]
		}
		var err error
		max, err = strconv.ParseFloat(maxStr, 64)
		if err != nil {
			return resp.Err("ERR max value is not a float")
		}
		if exclusive {
			max -= 1e-9
		}
	}

	removed, err := ops.ZRemRangeByScore(ctx, key, min, max)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(removed)
}

func (h *Handler) zremrangebyrankOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("zremrangebyrank")
	}

	key := args[0].Bulk
	start, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}

	removed, err := ops.ZRemRangeByRank(ctx, key, start, stop)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(removed)
}

func (h *Handler) zincrbyOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("zincrby")
	}

	key := args[0].Bulk
	increment, err := strconv.ParseFloat(args[1].Bulk, 64)
	if err != nil {
		return resp.Err("ERR value is not a valid float")
	}
	member := args[2].Bulk

	newScore, err := ops.ZIncrBy(ctx, key, increment, member)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Bulk(strconv.FormatFloat(newScore, 'f', -1, 64))
}

func (h *Handler) zpopminOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("zpopmin")
	}

	key := args[0].Bulk
	count := int64(1)

	if len(args) > 1 {
		var err error
		count, err = strconv.ParseInt(args[1].Bulk, 10, 64)
		if err != nil || count < 0 {
			return resp.Err("ERR value is not an integer or out of range")
		}
	}

	members, err := ops.ZPopMin(ctx, key, count)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	if len(members) == 0 {
		return resp.Arr()
	}

	result := make([]resp.Value, 0, len(members)*2)
	for _, m := range members {
		result = append(result, resp.Bulk(m.Member))
		result = append(result, resp.Bulk(strconv.FormatFloat(m.Score, 'f', -1, 64)))
	}
	return resp.Arr(result...)
}

func (h *Handler) zpopmaxOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("zpopmax")
	}

	key := args[0].Bulk
	count := int64(1)

	if len(args) > 1 {
		var err error
		count, err = strconv.ParseInt(args[1].Bulk, 10, 64)
		if err != nil || count < 0 {
			return resp.Err("ERR value is not an integer or out of range")
		}
	}

	members, err := ops.ZPopMax(ctx, key, count)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	if len(members) == 0 {
		return resp.Arr()
	}

	result := make([]resp.Value, 0, len(members)*2)
	for _, m := range members {
		result = append(result, resp.Bulk(m.Member))
		result = append(result, resp.Bulk(strconv.FormatFloat(m.Score, 'f', -1, 64)))
	}
	return resp.Arr(result...)
}

func (h *Handler) zrankOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("zrank")
	}

	key := args[0].Bulk
	member := args[1].Bulk

	rank, found, err := ops.ZRank(ctx, key, member)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Int(rank)
}

func (h *Handler) zrevrankOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("zrevrank")
	}

	key := args[0].Bulk
	member := args[1].Bulk

	rank, found, err := ops.ZRevRank(ctx, key, member)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Int(rank)
}

func (h *Handler) zcountOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("zcount")
	}

	key := args[0].Bulk
	minStr := args[1].Bulk
	maxStr := args[2].Bulk

	// Parse min score
	var min float64
	if minStr == "-inf" {
		min = -1e308
	} else if minStr == "+inf" || minStr == "inf" {
		min = 1e308
	} else {
		exclusive := strings.HasPrefix(minStr, "(")
		if exclusive {
			minStr = minStr[1:]
		}
		var err error
		min, err = strconv.ParseFloat(minStr, 64)
		if err != nil {
			return resp.Err("ERR min value is not a float")
		}
		if exclusive {
			min += 1e-9 // Approximate exclusivity
		}
	}

	// Parse max score
	var max float64
	if maxStr == "+inf" || maxStr == "inf" {
		max = 1e308
	} else if maxStr == "-inf" {
		max = -1e308
	} else {
		exclusive := strings.HasPrefix(maxStr, "(")
		if exclusive {
			maxStr = maxStr[1:]
		}
		var err error
		max, err = strconv.ParseFloat(maxStr, 64)
		if err != nil {
			return resp.Err("ERR max value is not a float")
		}
		if exclusive {
			max -= 1e-9 // Approximate exclusivity
		}
	}

	count, err := ops.ZCount(ctx, key, min, max)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) zscanOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("zscan")
	}

	key := args[0].Bulk
	cursor, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}

	pattern := "*"
	count := int64(10)

	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "MATCH":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			i++
			pattern = args[i].Bulk
		case "COUNT":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			i++
			count, err = strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
		}
	}

	nextCursor, members, err := ops.ZScan(ctx, key, cursor, pattern, count)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	// Build result array with member/score pairs
	items := make([]resp.Value, 0, len(members)*2)
	for _, m := range members {
		items = append(items, resp.Bulk(m.Member))
		items = append(items, resp.Bulk(strconv.FormatFloat(m.Score, 'f', -1, 64)))
	}

	return resp.Arr(
		resp.Bulk(strconv.FormatInt(nextCursor, 10)),
		resp.Arr(items...),
	)
}

func (h *Handler) zunionstoreOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("zunionstore")
	}

	destination := args[0].Bulk
	numKeys, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil || numKeys <= 0 {
		return resp.Err("ERR value is not an integer or out of range")
	}

	if len(args) < int(numKeys)+2 {
		return resp.ErrWrongArgs("zunionstore")
	}

	keys := make([]string, numKeys)
	for i := int64(0); i < numKeys; i++ {
		keys[i] = args[2+i].Bulk
	}

	// Parse optional WEIGHTS and AGGREGATE
	var weights []float64
	aggregate := "SUM"
	idx := int(numKeys) + 2

	for idx < len(args) {
		opt := strings.ToUpper(args[idx].Bulk)
		switch opt {
		case "WEIGHTS":
			idx++
			weights = make([]float64, numKeys)
			for i := int64(0); i < numKeys && idx < len(args); i++ {
				w, err := strconv.ParseFloat(args[idx].Bulk, 64)
				if err != nil {
					return resp.Err("ERR weight value is not a float")
				}
				weights[i] = w
				idx++
			}
		case "AGGREGATE":
			idx++
			if idx < len(args) {
				aggregate = strings.ToUpper(args[idx].Bulk)
				if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
					return resp.Err("ERR syntax error")
				}
				idx++
			}
		default:
			idx++
		}
	}

	count, err := ops.ZUnionStore(ctx, destination, keys, weights, aggregate)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) zinterstoreOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("zinterstore")
	}

	destination := args[0].Bulk
	numKeys, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil || numKeys <= 0 {
		return resp.Err("ERR value is not an integer or out of range")
	}

	if len(args) < int(numKeys)+2 {
		return resp.ErrWrongArgs("zinterstore")
	}

	keys := make([]string, numKeys)
	for i := int64(0); i < numKeys; i++ {
		keys[i] = args[2+i].Bulk
	}

	// Parse optional WEIGHTS and AGGREGATE
	var weights []float64
	aggregate := "SUM"
	idx := int(numKeys) + 2

	for idx < len(args) {
		opt := strings.ToUpper(args[idx].Bulk)
		switch opt {
		case "WEIGHTS":
			idx++
			weights = make([]float64, numKeys)
			for i := int64(0); i < numKeys && idx < len(args); i++ {
				w, err := strconv.ParseFloat(args[idx].Bulk, 64)
				if err != nil {
					return resp.Err("ERR weight value is not a float")
				}
				weights[i] = w
				idx++
			}
		case "AGGREGATE":
			idx++
			if idx < len(args) {
				aggregate = strings.ToUpper(args[idx].Bulk)
				if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
					return resp.Err("ERR syntax error")
				}
				idx++
			}
		default:
			idx++
		}
	}

	count, err := ops.ZInterStore(ctx, destination, keys, weights, aggregate)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) lremOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("lrem")
	}

	key := args[0].Bulk
	count, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	element := args[2].Bulk

	removed, err := ops.LRem(ctx, key, count, element)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(removed)
}

func (h *Handler) ltrimOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("ltrim")
	}

	key := args[0].Bulk
	start, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}

	err = ops.LTrim(ctx, key, start, stop)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) rpoplpushOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("rpoplpush")
	}

	source := args[0].Bulk
	destination := args[1].Bulk

	value, found, err := ops.RPopLPush(ctx, source, destination)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) lposOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("lpos")
	}

	key := args[0].Bulk
	element := args[1].Bulk
	var rank, count, maxlen int64 = 1, 1, 0

	// Parse options
	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "RANK":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			i++
			var err error
			rank, err = strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
		case "COUNT":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			i++
			var err error
			count, err = strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
		case "MAXLEN":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			i++
			var err error
			maxlen, err = strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
		}
	}

	positions, err := ops.LPos(ctx, key, element, rank, count, maxlen)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	if len(positions) == 0 {
		return resp.NullBulk()
	}
	if count == 1 {
		return resp.Int(positions[0])
	}

	result := make([]resp.Value, len(positions))
	for i, pos := range positions {
		result[i] = resp.Int(pos)
	}
	return resp.Arr(result...)
}

func (h *Handler) lsetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("lset")
	}

	index, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}

	err = ops.LSet(ctx, args[0].Bulk, index, args[2].Bulk)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) linsertOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 4 {
		return resp.ErrWrongArgs("linsert")
	}

	key := args[0].Bulk
	position := strings.ToUpper(args[1].Bulk)
	pivot := args[2].Bulk
	element := args[3].Bulk

	var before bool
	switch position {
	case "BEFORE":
		before = true
	case "AFTER":
		before = false
	default:
		return resp.Err("ERR syntax error")
	}

	length, err := ops.LInsert(ctx, key, pivot, element, before)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

// ============== Set Extensions ==============

func (h *Handler) smismemberOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("smismember")
	}

	key := args[0].Bulk
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = args[i].Bulk
	}

	results, err := ops.SMIsMember(ctx, key, members)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	respResults := make([]resp.Value, len(results))
	for i, exists := range results {
		if exists {
			respResults[i] = resp.Int(1)
		} else {
			respResults[i] = resp.Int(0)
		}
	}
	return resp.Arr(respResults...)
}

func (h *Handler) sinterOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("sinter")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	members, err := ops.SInter(ctx, keys)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.Bulk(m)
	}
	return resp.Arr(result...)
}

// SINTERCARD numkeys key [key ...] [LIMIT limit]
func (h *Handler) sintercardOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("sintercard")
	}
	numkeys, err := strconv.ParseInt(args[0].Bulk, 10, 64)
	if err != nil || numkeys < 1 {
		return resp.Err("ERR numkeys should be greater than 0")
	}
	if int64(len(args)) < 1+numkeys {
		return resp.ErrWrongArgs("sintercard")
	}
	keys := make([]string, numkeys)
	for i := int64(0); i < numkeys; i++ {
		keys[i] = args[1+i].Bulk
	}
	var limit int64
	// Optional LIMIT <n>
	rest := args[1+numkeys:]
	if len(rest) > 0 {
		if len(rest) != 2 || strings.ToUpper(rest[0].Bulk) != "LIMIT" {
			return resp.Err("ERR syntax error")
		}
		limit, err = strconv.ParseInt(rest[1].Bulk, 10, 64)
		if err != nil || limit < 0 {
			return resp.Err("ERR LIMIT can't be negative")
		}
	}

	n, err := ops.SInterCard(ctx, keys, limit)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(n)
}

func (h *Handler) sinterstoreOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("sinterstore")
	}

	destination := args[0].Bulk
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = args[i].Bulk
	}

	count, err := ops.SInterStore(ctx, destination, keys)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) sunionOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("sunion")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	members, err := ops.SUnion(ctx, keys)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.Bulk(m)
	}
	return resp.Arr(result...)
}

func (h *Handler) sunionstoreOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("sunionstore")
	}

	destination := args[0].Bulk
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = args[i].Bulk
	}

	count, err := ops.SUnionStore(ctx, destination, keys)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) sdiffOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("sdiff")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	members, err := ops.SDiff(ctx, keys)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.Bulk(m)
	}
	return resp.Arr(result...)
}

func (h *Handler) sdiffstoreOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("sdiffstore")
	}

	destination := args[0].Bulk
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = args[i].Bulk
	}

	count, err := ops.SDiffStore(ctx, destination, keys)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) sscanOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("sscan")
	}

	key := args[0].Bulk
	cursor, err := strconv.ParseUint(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("ERR invalid cursor")
	}

	// Parse options
	pattern := "*"
	count := int64(10)

	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "MATCH":
			if i+1 >= len(args) {
				return resp.ErrWrongArgs("sscan")
			}
			pattern = args[i+1].Bulk
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return resp.ErrWrongArgs("sscan")
			}
			count, err = strconv.ParseInt(args[i+1].Bulk, 10, 64)
			if err != nil {
				return resp.Err("ERR value is not an integer or out of range")
			}
			i++
		}
	}

	// Get all members
	allMembers, err := ops.SMembers(ctx, key)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	// Filter by pattern
	var filteredMembers []string
	for _, member := range allMembers {
		matched, _ := matchGlob(pattern, member)
		if matched {
			filteredMembers = append(filteredMembers, member)
		}
	}

	// Simulate cursor-based pagination
	start := int(cursor)
	if start >= len(filteredMembers) {
		return resp.Arr(resp.Bulk("0"), resp.Arr())
	}

	end := start + int(count)
	if end > len(filteredMembers) {
		end = len(filteredMembers)
	}

	resultMembers := filteredMembers[start:end]

	var nextCursor string
	if end >= len(filteredMembers) {
		nextCursor = "0"
	} else {
		nextCursor = strconv.Itoa(end)
	}

	memberValues := make([]resp.Value, len(resultMembers))
	for i, member := range resultMembers {
		memberValues[i] = resp.Bulk(member)
	}

	return resp.Arr(resp.Bulk(nextCursor), resp.Arr(memberValues...))
}

func (h *Handler) unlinkOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	// UNLINK is like DEL but async - we just implement it as DEL
	return h.delOp(ctx, ops, args)
}

// ============== HyperLogLog Commands ==============

func (h *Handler) pfaddOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("pfadd")
	}

	key := args[0].Bulk
	elements := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		elements[i-1] = args[i].Bulk
	}

	changed, err := ops.PFAdd(ctx, key, elements)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(changed)
}

func (h *Handler) pfcountOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("pfcount")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	count, err := ops.PFCount(ctx, keys)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) pfmergeOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("pfmerge")
	}

	destKey := args[0].Bulk
	sourceKeys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		sourceKeys[i-1] = args[i].Bulk
	}

	err := ops.PFMerge(ctx, destKey, sourceKeys)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.OK()
}

// ============== Bitmap Commands ==============

func (h *Handler) setbitOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("setbit")
	}

	key := args[0].Bulk
	offset, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil || offset < 0 {
		return resp.Err("ERR bit offset is not an integer or out of range")
	}

	value, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil || (value != 0 && value != 1) {
		return resp.Err("ERR bit is not an integer or out of range")
	}

	oldBit, err := ops.SetBit(ctx, key, offset, int(value))
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(oldBit)
}

func (h *Handler) getbitOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("getbit")
	}

	key := args[0].Bulk
	offset, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil || offset < 0 {
		return resp.Err("ERR bit offset is not an integer or out of range")
	}

	bit, err := ops.GetBit(ctx, key, offset)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(bit)
}

func (h *Handler) bitcountOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("bitcount")
	}

	key := args[0].Bulk
	var start, end int64 = 0, -1
	useBit := false

	if len(args) >= 3 {
		var err error
		start, err = strconv.ParseInt(args[1].Bulk, 10, 64)
		if err != nil {
			return resp.Err("ERR value is not an integer or out of range")
		}
		end, err = strconv.ParseInt(args[2].Bulk, 10, 64)
		if err != nil {
			return resp.Err("ERR value is not an integer or out of range")
		}
	}

	if len(args) >= 4 {
		unit := strings.ToUpper(args[3].Bulk)
		if unit != "BYTE" && unit != "BIT" {
			return resp.Err("ERR syntax error")
		}
		useBit = unit == "BIT"
	}

	count, err := ops.BitCount(ctx, key, start, end, useBit)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) bitopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("bitop")
	}

	operation := strings.ToUpper(args[0].Bulk)
	destKey := args[1].Bulk
	keys := make([]string, len(args)-2)
	for i := 2; i < len(args); i++ {
		keys[i-2] = args[i].Bulk
	}

	if operation != "AND" && operation != "OR" && operation != "XOR" && operation != "NOT" {
		return resp.Err("ERR syntax error")
	}

	if operation == "NOT" && len(keys) != 1 {
		return resp.Err("ERR BITOP NOT requires one and only one key")
	}

	length, err := ops.BitOp(ctx, operation, destKey, keys)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) bitposOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("bitpos")
	}

	key := args[0].Bulk
	bit, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil || (bit != 0 && bit != 1) {
		return resp.Err("ERR bit is not an integer or out of range")
	}

	var start, end int64 = 0, -1
	useBit := false

	if len(args) >= 3 {
		start, err = strconv.ParseInt(args[2].Bulk, 10, 64)
		if err != nil {
			return resp.Err("ERR value is not an integer or out of range")
		}
	}

	if len(args) >= 4 {
		end, err = strconv.ParseInt(args[3].Bulk, 10, 64)
		if err != nil {
			return resp.Err("ERR value is not an integer or out of range")
		}
	}

	if len(args) >= 5 {
		unit := strings.ToUpper(args[4].Bulk)
		if unit != "BYTE" && unit != "BIT" {
			return resp.Err("ERR syntax error")
		}
		useBit = unit == "BIT"
	}

	pos, err := ops.BitPos(ctx, key, int(bit), start, end, useBit)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(pos)
}

// ============== Server Commands ==============

func (h *Handler) infoOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	uptime := time.Since(h.startTime)
	dbSize, _ := ops.DBSize(ctx)

	info := fmt.Sprintf(`# Server
redis_version:7.0.0-postkeys
os:%s
arch:%s

# Stats
uptime_in_seconds:%d
uptime_in_days:%d

# Keyspace
db0:keys=%d
`, runtime.GOOS, runtime.GOARCH, int(uptime.Seconds()), int(uptime.Hours()/24), dbSize)

	return resp.Bulk(info)
}

func (h *Handler) dbsizeOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	size, err := ops.DBSize(ctx)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(size)
}

// ExecuteWithOps executes a command using the provided Operations interface.
// This is the unified command execution that works for both regular and transaction contexts.
func (h *Handler) ExecuteWithOps(ctx context.Context, ops storage.Operations, cmdName string, args []resp.Value) resp.Value {
	switch cmdName {
	// String commands
	case "GET":
		return h.getOp(ctx, ops, args)
	case "SET":
		return h.setOp(ctx, ops, args)
	case "SETNX":
		return h.setnxOp(ctx, ops, args)
	case "SETEX":
		return h.setexOp(ctx, ops, args)
	case "MGET":
		return h.mgetOp(ctx, ops, args)
	case "MSET":
		return h.msetOp(ctx, ops, args)
	case "INCR":
		return h.incrOp(ctx, ops, args)
	case "DECR":
		return h.decrOp(ctx, ops, args)
	case "INCRBY":
		return h.incrbyOp(ctx, ops, args)
	case "DECRBY":
		return h.decrbyOp(ctx, ops, args)
	case "INCRBYFLOAT":
		return h.incrbyfloatOp(ctx, ops, args)
	case "APPEND":
		return h.appendCmdOp(ctx, ops, args)
	case "GETRANGE":
		return h.getrangeOp(ctx, ops, args)
	case "SETRANGE":
		return h.setrangeOp(ctx, ops, args)
	case "STRLEN":
		return h.strlenOp(ctx, ops, args)
	case "GETEX":
		return h.getexOp(ctx, ops, args)
	case "GETDEL":
		return h.getdelOp(ctx, ops, args)
	case "GETSET":
		return h.getsetOp(ctx, ops, args)
	case "BITFIELD":
		return h.bitfieldOp(ctx, ops, args)

	// Key commands
	case "DEL":
		return h.delOp(ctx, ops, args)
	case "EXISTS":
		return h.existsOp(ctx, ops, args)
	case "EXPIRE":
		return h.expireOp(ctx, ops, args)
	case "PEXPIRE":
		return h.pexpireOp(ctx, ops, args)
	case "EXPIREAT":
		return h.expireatOp(ctx, ops, args)
	case "PEXPIREAT":
		return h.pexpireatOp(ctx, ops, args)
	case "TTL":
		return h.ttlOp(ctx, ops, args)
	case "PTTL":
		return h.pttlOp(ctx, ops, args)
	case "PERSIST":
		return h.persistOp(ctx, ops, args)
	case "KEYS":
		return h.keysOp(ctx, ops, args)
	case "TYPE":
		return h.typeCmdOp(ctx, ops, args)
	case "RENAME":
		return h.renameOp(ctx, ops, args)
	case "COPY":
		return h.copyOp(ctx, ops, args)
	case "RANDOMKEY":
		return h.randomKeyOp(ctx, ops, args)
	case "OBJECT":
		return h.objectOp(ctx, ops, args)

	// Hash commands
	case "HGET":
		return h.hgetOp(ctx, ops, args)
	case "HSET":
		return h.hsetOp(ctx, ops, args)
	case "HDEL":
		return h.hdelOp(ctx, ops, args)
	case "HGETALL":
		return h.hgetallOp(ctx, ops, args)
	case "HMGET":
		return h.hmgetOp(ctx, ops, args)
	case "HMSET":
		return h.hmsetOp(ctx, ops, args)
	case "HEXISTS":
		return h.hexistsOp(ctx, ops, args)
	case "HKEYS":
		return h.hkeysOp(ctx, ops, args)
	case "HVALS":
		return h.hvalsOp(ctx, ops, args)
	case "HLEN":
		return h.hlenOp(ctx, ops, args)
	case "HINCRBY":
		return h.hincrbyOp(ctx, ops, args)
	case "HINCRBYFLOAT":
		return h.hincrbyfloatOp(ctx, ops, args)
	case "HSETNX":
		return h.hsetnxOp(ctx, ops, args)
	case "HSCAN":
		return h.hscanOp(ctx, ops, args)
	case "HRANDFIELD":
		return h.hrandfieldOp(ctx, ops, args)

	// Watch commands (no-ops for PostgreSQL compatibility)
	case "WATCH":
		return h.watchOp(ctx, ops, args)
	case "UNWATCH":
		return h.unwatchOp(ctx, ops, args)

	// List commands
	case "LPUSH":
		return h.lpushOp(ctx, ops, args)
	case "RPUSH":
		return h.rpushOp(ctx, ops, args)
	case "LPOP":
		return h.lpopOp(ctx, ops, args)
	case "RPOP":
		return h.rpopOp(ctx, ops, args)
	case "BLPOP":
		return h.blpopOp(ctx, ops, args)
	case "BRPOP":
		return h.brpopOp(ctx, ops, args)
	case "LMPOP":
		return h.lmpopOp(ctx, ops, args)
	case "BLMPOP":
		return h.blmpopOp(ctx, ops, args)
	case "LLEN":
		return h.llenOp(ctx, ops, args)
	case "LRANGE":
		return h.lrangeOp(ctx, ops, args)
	case "LINDEX":
		return h.lindexOp(ctx, ops, args)
	case "LREM":
		return h.lremOp(ctx, ops, args)
	case "LTRIM":
		return h.ltrimOp(ctx, ops, args)
	case "RPOPLPUSH":
		return h.rpoplpushOp(ctx, ops, args)
	case "LPOS":
		return h.lposOp(ctx, ops, args)
	case "LSET":
		return h.lsetOp(ctx, ops, args)
	case "LINSERT":
		return h.linsertOp(ctx, ops, args)

	// Key scan commands
	case "SCAN":
		return h.scanOp(ctx, ops, args)

	// Set commands
	case "SADD":
		return h.saddOp(ctx, ops, args)
	case "SREM":
		return h.sremOp(ctx, ops, args)
	case "SMEMBERS":
		return h.smembersOp(ctx, ops, args)
	case "SISMEMBER":
		return h.sismemberOp(ctx, ops, args)
	case "SCARD":
		return h.scardOp(ctx, ops, args)
	case "SMISMEMBER":
		return h.smismemberOp(ctx, ops, args)
	case "SINTER":
		return h.sinterOp(ctx, ops, args)
	case "SINTERSTORE":
		return h.sinterstoreOp(ctx, ops, args)
	case "SUNION":
		return h.sunionOp(ctx, ops, args)
	case "SUNIONSTORE":
		return h.sunionstoreOp(ctx, ops, args)
	case "SDIFF":
		return h.sdiffOp(ctx, ops, args)
	case "SDIFFSTORE":
		return h.sdiffstoreOp(ctx, ops, args)
	case "SINTERCARD":
		return h.sintercardOp(ctx, ops, args)
	case "SRANDMEMBER":
		return h.srandmemberOp(ctx, ops, args)
	case "SSCAN":
		return h.sscanOp(ctx, ops, args)

	// Key commands (additional)
	case "UNLINK":
		return h.unlinkOp(ctx, ops, args)

	// Sorted set commands
	case "ZADD":
		return h.zaddOp(ctx, ops, args)
	case "ZRANGE":
		return h.zrangeOp(ctx, ops, args)
	case "ZRANGEBYSCORE":
		return h.zrangebyscoreOp(ctx, ops, args)
	case "ZREVRANGE":
		return h.zrevrangeOp(ctx, ops, args)
	case "ZREVRANGEBYSCORE":
		return h.zrevrangebyscoreOp(ctx, ops, args)
	case "ZRANGEBYLEX":
		return h.zrangebylexOp(ctx, ops, args)
	case "ZREVRANGEBYLEX":
		return h.zrevrangebylexOp(ctx, ops, args)
	case "ZLEXCOUNT":
		return h.zlexcountOp(ctx, ops, args)
	case "ZRANGESTORE":
		return h.zrangestoreOp(ctx, ops, args)
	case "ZRANDMEMBER":
		return h.zrandmemberOp(ctx, ops, args)
	case "ZMPOP":
		return h.zmpopOp(ctx, ops, args)
	case "BZMPOP":
		return h.bzmpopOp(ctx, ops, args)
	case "ZSCORE":
		return h.zscoreOp(ctx, ops, args)
	case "ZREM":
		return h.zremOp(ctx, ops, args)
	case "ZREMRANGEBYSCORE":
		return h.zremrangebyscoreOp(ctx, ops, args)
	case "ZREMRANGEBYRANK":
		return h.zremrangebyrankOp(ctx, ops, args)
	case "ZCARD":
		return h.zcardOp(ctx, ops, args)
	case "ZINCRBY":
		return h.zincrbyOp(ctx, ops, args)
	case "ZPOPMIN":
		return h.zpopminOp(ctx, ops, args)
	case "ZPOPMAX":
		return h.zpopmaxOp(ctx, ops, args)
	case "ZRANK":
		return h.zrankOp(ctx, ops, args)
	case "ZREVRANK":
		return h.zrevrankOp(ctx, ops, args)
	case "ZCOUNT":
		return h.zcountOp(ctx, ops, args)
	case "ZSCAN":
		return h.zscanOp(ctx, ops, args)
	case "ZUNIONSTORE":
		return h.zunionstoreOp(ctx, ops, args)
	case "ZINTERSTORE":
		return h.zinterstoreOp(ctx, ops, args)

	// HyperLogLog commands
	case "PFADD":
		return h.pfaddOp(ctx, ops, args)
	case "PFCOUNT":
		return h.pfcountOp(ctx, ops, args)
	case "PFMERGE":
		return h.pfmergeOp(ctx, ops, args)

	// Bitmap commands
	case "SETBIT":
		return h.setbitOp(ctx, ops, args)
	case "GETBIT":
		return h.getbitOp(ctx, ops, args)
	case "BITCOUNT":
		return h.bitcountOp(ctx, ops, args)
	case "BITOP":
		return h.bitopOp(ctx, ops, args)
	case "BITPOS":
		return h.bitposOp(ctx, ops, args)

	// Server commands
	case "INFO":
		return h.infoOp(ctx, ops, args)
	case "DBSIZE":
		return h.dbsizeOp(ctx, ops, args)

	// Scripting commands
	case "EVAL":
		return h.evalOp(ctx, ops, args)
	case "EVALSHA":
		return h.evalshaOp(ctx, ops, args)
	case "SCRIPT":
		return h.scriptOp(ctx, ops, args)

	default:
		return resp.Err(fmt.Sprintf("unknown command '%s'", cmdName))
	}
}
