// Package handler implements Redis command handlers.
// This file implements Lua scripting support for EVAL/EVALSHA/SCRIPT commands.
package handler

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/mnorrsken/postkeys/internal/resp"
	"github.com/mnorrsken/postkeys/internal/storage"
	lua "github.com/yuin/gopher-lua"
)

// ScriptCache holds cached Lua scripts indexed by SHA1
type ScriptCache struct {
	mu      sync.RWMutex
	scripts map[string]string // SHA1 -> script source
}

// Global script cache
var scriptCache = &ScriptCache{
	scripts: make(map[string]string),
}

// scriptSHA1 computes the SHA1 hash of a script
func scriptSHA1(script string) string {
	h := sha1.New()
	h.Write([]byte(script))
	return hex.EncodeToString(h.Sum(nil))
}

// Get retrieves a script by SHA1
func (sc *ScriptCache) Get(sha1 string) (string, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	script, ok := sc.scripts[strings.ToLower(sha1)]
	return script, ok
}

// Store caches a script and returns its SHA1
func (sc *ScriptCache) Store(script string) string {
	sha := scriptSHA1(script)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.scripts[sha] = script
	return sha
}

// Exists checks if scripts exist by SHA1
func (sc *ScriptCache) Exists(shas []string) []bool {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	results := make([]bool, len(shas))
	for i, sha := range shas {
		_, results[i] = sc.scripts[strings.ToLower(sha)]
	}
	return results
}

// Flush clears all cached scripts
func (sc *ScriptCache) Flush() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.scripts = make(map[string]string)
}

// luaExecutor executes Lua scripts with Redis command access
type luaExecutor struct {
	ctx  context.Context
	h    *Handler
	ops  storage.Operations
	keys []string
	argv []string
}

// newLuaExecutor creates a new Lua executor
func newLuaExecutor(ctx context.Context, h *Handler, ops storage.Operations, keys, argv []string) *luaExecutor {
	return &luaExecutor{
		ctx:  ctx,
		h:    h,
		ops:  ops,
		keys: keys,
		argv: argv,
	}
}

// openSafeLibs loads only libraries that are safe for untrusted scripts.
// This matches Redis behavior: os, io, debug, package, and loadfile/dofile are excluded.
func openSafeLibs(L *lua.LState) {
	for _, pair := range []struct {
		name string
		fn   lua.LGFunction
	}{
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
		{lua.StringLibName, lua.OpenString},
		{lua.MathLibName, lua.OpenMath},
		{lua.CoroutineLibName, lua.OpenCoroutine},
	} {
		L.Push(L.NewFunction(pair.fn))
		L.Push(lua.LString(pair.name))
		L.Call(1, 0)
	}

	// Remove dangerous base functions that can access the filesystem
	L.SetGlobal("dofile", lua.LNil)
	L.SetGlobal("loadfile", lua.LNil)
}

// Execute runs a Lua script and returns the result
func (le *luaExecutor) Execute(script string) (resp.Value, error) {
	L := lua.NewState(lua.Options{
		SkipOpenLibs: true,
	})
	defer L.Close()

	// Load only safe libraries (no os, io, debug, or package)
	openSafeLibs(L)

	// Set up the redis table with call and pcall
	redisTable := L.NewTable()
	L.SetField(redisTable, "call", L.NewFunction(le.redisCall))
	L.SetField(redisTable, "pcall", L.NewFunction(le.redisPCall))
	L.SetField(redisTable, "error_reply", L.NewFunction(le.redisErrorReply))
	L.SetField(redisTable, "status_reply", L.NewFunction(le.redisStatusReply))
	L.SetField(redisTable, "log", L.NewFunction(le.redisLog))
	L.SetField(redisTable, "sha1hex", L.NewFunction(le.redisSha1Hex))
	L.SetGlobal("redis", redisTable)

	// Set up KEYS table
	keysTable := L.NewTable()
	for i, k := range le.keys {
		L.RawSetInt(keysTable, i+1, lua.LString(k))
	}
	L.SetGlobal("KEYS", keysTable)

	// Set up ARGV table
	argvTable := L.NewTable()
	for i, a := range le.argv {
		L.RawSetInt(argvTable, i+1, lua.LString(a))
	}
	L.SetGlobal("ARGV", argvTable)

	// Execute the script
	if err := L.DoString(script); err != nil {
		return resp.Value{}, fmt.Errorf("ERR Error running script: %v", err)
	}

	// Get the return value (if any)
	// Check stack size - script may not return anything
	top := L.GetTop()
	if top == 0 {
		// No return value, return nil
		return resp.NullBulk(), nil
	}

	result := L.Get(-1)
	L.Pop(1)

	return le.luaToResp(result), nil
}

// redisCall implements redis.call() - raises error on Redis errors
func (le *luaExecutor) redisCall(L *lua.LState) int {
	result := le.executeRedisCommand(L)
	if result.Type == resp.Error {
		L.RaiseError("%s", result.Str)
		return 0
	}
	L.Push(le.respToLua(L, result))
	return 1
}

// redisPCall implements redis.pcall() - returns error as table instead of raising
func (le *luaExecutor) redisPCall(L *lua.LState) int {
	result := le.executeRedisCommand(L)
	L.Push(le.respToLua(L, result))
	return 1
}

// redisErrorReply creates an error reply table
func (le *luaExecutor) redisErrorReply(L *lua.LState) int {
	msg := L.CheckString(1)
	t := L.NewTable()
	L.SetField(t, "err", lua.LString(msg))
	L.Push(t)
	return 1
}

// redisStatusReply creates a status reply table
func (le *luaExecutor) redisStatusReply(L *lua.LState) int {
	msg := L.CheckString(1)
	t := L.NewTable()
	L.SetField(t, "ok", lua.LString(msg))
	L.Push(t)
	return 1
}

// redisLog implements redis.log() - currently a no-op but accepts args
func (le *luaExecutor) redisLog(L *lua.LState) int {
	// No-op for now, but we accept the arguments
	return 0
}

// redisSha1Hex implements redis.sha1hex() - compute SHA1 of a string
func (le *luaExecutor) redisSha1Hex(L *lua.LState) int {
	s := L.CheckString(1)
	L.Push(lua.LString(scriptSHA1(s)))
	return 1
}

// executeRedisCommand executes a Redis command from Lua
func (le *luaExecutor) executeRedisCommand(L *lua.LState) resp.Value {
	nargs := L.GetTop()
	if nargs == 0 {
		return resp.Err("ERR Please specify at least one argument for redis.call()")
	}

	// Get command name
	cmdName := strings.ToUpper(L.CheckString(1))

	// Build args
	args := make([]resp.Value, nargs-1)
	for i := 2; i <= nargs; i++ {
		arg := L.Get(i)
		args[i-2] = resp.Bulk(le.luaToString(arg))
	}

	// Block certain commands in scripts
	switch cmdName {
	case "SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "PUBLISH":
		return resp.Err("ERR This Redis command is not allowed from a script")
	case "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH":
		return resp.Err("ERR This Redis command is not allowed from a script")
	case "EVAL", "EVALSHA", "SCRIPT":
		return resp.Err("ERR This Redis command is not allowed from a script")
	}

	// Execute the command
	return le.h.ExecuteWithOps(le.ctx, le.ops, cmdName, args)
}

// luaToString converts a Lua value to a string
func (le *luaExecutor) luaToString(v lua.LValue) string {
	switch val := v.(type) {
	case lua.LString:
		return string(val)
	case lua.LNumber:
		// Format as integer if it's a whole number
		f := float64(val)
		if f == float64(int64(f)) {
			return strconv.FormatInt(int64(f), 10)
		}
		return strconv.FormatFloat(f, 'f', -1, 64)
	case lua.LBool:
		if val {
			return "1"
		}
		return "0"
	case *lua.LNilType:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

// respToLua converts a RESP value to a Lua value
func (le *luaExecutor) respToLua(L *lua.LState, v resp.Value) lua.LValue {
	switch v.Type {
	case resp.SimpleString:
		t := L.NewTable()
		L.SetField(t, "ok", lua.LString(v.Str))
		return t
	case resp.Error:
		t := L.NewTable()
		L.SetField(t, "err", lua.LString(v.Str))
		return t
	case resp.Integer:
		return lua.LNumber(v.Num)
	case resp.BulkString:
		if v.Bulk == "" && v.Null {
			return lua.LFalse
		}
		return lua.LString(v.Bulk)
	case resp.Array:
		if v.Array == nil {
			return lua.LFalse
		}
		t := L.NewTable()
		for i, item := range v.Array {
			L.RawSetInt(t, i+1, le.respToLua(L, item))
		}
		return t
	case resp.Null:
		return lua.LFalse
	default:
		return lua.LNil
	}
}

// luaToResp converts a Lua value to a RESP value
func (le *luaExecutor) luaToResp(v lua.LValue) resp.Value {
	switch val := v.(type) {
	case lua.LString:
		return resp.Bulk(string(val))
	case lua.LNumber:
		// Return as integer if whole number, otherwise as bulk string
		f := float64(val)
		if f == float64(int64(f)) {
			return resp.Int(int64(f))
		}
		return resp.Bulk(strconv.FormatFloat(f, 'f', -1, 64))
	case lua.LBool:
		if val {
			return resp.Int(1)
		}
		return resp.NullBulk()
	case *lua.LNilType:
		return resp.NullBulk()
	case *lua.LTable:
		// Check for error or status reply
		if err := val.RawGetString("err"); err != lua.LNil {
			return resp.Err(le.luaToString(err))
		}
		if ok := val.RawGetString("ok"); ok != lua.LNil {
			return resp.Value{Type: resp.SimpleString, Str: le.luaToString(ok)}
		}

		// Convert to array
		arr := make([]resp.Value, 0)
		val.ForEach(func(k, v lua.LValue) {
			// Only include integer-keyed entries (Lua array behavior)
			if _, ok := k.(lua.LNumber); ok {
				arr = append(arr, le.luaToResp(v))
			}
		})
		return resp.Arr(arr...)
	default:
		return resp.NullBulk()
	}
}

// ============== EVAL/EVALSHA/SCRIPT Command Handlers ==============

// evalOp handles EVAL command
func (h *Handler) evalOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("eval")
	}

	script := args[0].Bulk
	numKeys, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}

	if numKeys < 0 {
		return resp.Err("ERR Number of keys can't be negative")
	}

	if len(args) < 2+numKeys {
		return resp.Err("ERR Number of keys can't be greater than number of args")
	}

	// Extract KEYS and ARGV
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = args[2+i].Bulk
	}

	argv := make([]string, len(args)-2-numKeys)
	for i := 0; i < len(argv); i++ {
		argv[i] = args[2+numKeys+i].Bulk
	}

	// Cache the script (EVAL always caches)
	scriptCache.Store(script)

	// Execute
	executor := newLuaExecutor(ctx, h, ops, keys, argv)
	result, err := executor.Execute(script)
	if err != nil {
		return resp.Err(err.Error())
	}

	return result
}

// evalshaOp handles EVALSHA command
func (h *Handler) evalshaOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("evalsha")
	}

	sha := args[0].Bulk
	numKeys, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}

	if numKeys < 0 {
		return resp.Err("ERR Number of keys can't be negative")
	}

	if len(args) < 2+numKeys {
		return resp.Err("ERR Number of keys can't be greater than number of args")
	}

	// Look up the script
	script, ok := scriptCache.Get(sha)
	if !ok {
		return resp.ErrCustom("NOSCRIPT No matching script. Please use EVAL.")
	}

	// Extract KEYS and ARGV
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = args[2+i].Bulk
	}

	argv := make([]string, len(args)-2-numKeys)
	for i := 0; i < len(argv); i++ {
		argv[i] = args[2+numKeys+i].Bulk
	}

	// Execute
	executor := newLuaExecutor(ctx, h, ops, keys, argv)
	result, err := executor.Execute(script)
	if err != nil {
		return resp.Err(err.Error())
	}

	return result
}

// scriptOp handles SCRIPT subcommands
func (h *Handler) scriptOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("script")
	}

	subCmd := strings.ToUpper(args[0].Bulk)
	subArgs := args[1:]

	switch subCmd {
	case "LOAD":
		if len(subArgs) != 1 {
			return resp.ErrWrongArgs("script load")
		}
		sha := scriptCache.Store(subArgs[0].Bulk)
		return resp.Bulk(sha)

	case "EXISTS":
		if len(subArgs) == 0 {
			return resp.ErrWrongArgs("script exists")
		}
		shas := make([]string, len(subArgs))
		for i, arg := range subArgs {
			shas[i] = arg.Bulk
		}
		results := scriptCache.Exists(shas)
		arr := make([]resp.Value, len(results))
		for i, exists := range results {
			if exists {
				arr[i] = resp.Int(1)
			} else {
				arr[i] = resp.Int(0)
			}
		}
		return resp.Arr(arr...)

	case "FLUSH":
		// Parse optional ASYNC/SYNC flag (we ignore it since our flush is instant)
		scriptCache.Flush()
		return resp.OK()

	case "KILL":
		// We don't have long-running scripts to kill, just return OK
		return resp.Err("NOTBUSY No scripts in execution right now.")

	case "DEBUG":
		// Scripting debug is not supported
		return resp.OK()

	default:
		return resp.Err(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'", subCmd))
	}
}
