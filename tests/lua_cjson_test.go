//go:build postgres
// +build postgres

package integration_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// This is Medplum's FHIRcast compare-and-set script, including its literal dotted key.
const fhircastCompareAndSet = `
local stored = redis.call('GET', KEYS[1])
if not stored then
  return 0
end
local ok, context = pcall(cjson.decode, stored)
if not ok or context['context.versionId'] ~= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[2])
return 1
`

func TestCJSONFHIRcastCompareAndSet(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()

	for _, mode := range []string{"EVAL", "EVALSHA"} {
		t.Run(mode, func(t *testing.T) {
			var sha string
			if mode == "EVALSHA" {
				var err error
				sha, err = ts.client.ScriptLoad(ctx, fhircastCompareAndSet).Result()
				if err != nil {
					t.Fatalf("SCRIPT LOAD: %v", err)
				}
			}
			key := "fhircast:" + mode
			const replacement = `{"context.versionId":"v2","payload":"replacement"}`
			run := func(t *testing.T, expected string, want int64) {
				t.Helper()
				var cmd *redis.Cmd
				if mode == "EVAL" {
					cmd = ts.client.Eval(ctx, fhircastCompareAndSet, []string{key}, expected, replacement)
				} else {
					cmd = ts.client.EvalSha(ctx, sha, []string{key}, expected, replacement)
				}
				got, err := cmd.Int64()
				if err != nil || got != want {
					t.Fatalf("%s returned %d, %v; want %d", mode, got, err, want)
				}
			}
			check := func(t *testing.T, want string) {
				t.Helper()
				got, err := ts.client.Get(ctx, key).Result()
				if err != nil || got != want {
					t.Fatalf("GET returned %q, %v; want %q", got, err, want)
				}
			}
			set := func(t *testing.T, value string) {
				t.Helper()
				if err := ts.client.Set(ctx, key, value, 0).Err(); err != nil {
					t.Fatalf("SET: %v", err)
				}
			}

			run(t, "v1", 0)
			if _, err := ts.client.Get(ctx, key).Result(); err != redis.Nil {
				t.Fatalf("missing key GET error = %v; want redis.Nil", err)
			}
			set(t, `{"context.versionId":"v1"}`)
			run(t, "v1", 1)
			check(t, replacement)
			run(t, "v1", 0)
			check(t, replacement)
			for _, tc := range []struct{ name, stored, expected string }{
				{"malformed", `{invalid`, "v1"},
				{"missing field", `{"other":"v1"}`, "v1"},
				{"numeric version", `{"context.versionId":1}`, "1"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					set(t, tc.stored)
					run(t, tc.expected, 0)
					check(t, tc.stored)
				})
			}
		})
	}
}

func TestCJSONBinaryStorageAndNullArguments(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()
	ctx := context.Background()
	const key = "cjson:binary"
	binary := string([]byte{0, 1, 127, 128, 254, 255}) + "/<>&"
	script := `
  redis.call('SET', KEYS[1], cjson.encode(ARGV[1]))
  local decoded = cjson.decode(redis.call('GET', KEYS[1]))
  local result = redis.pcall('SET', KEYS[1], cjson.null)
  assert(result.err == 'ERR Lua redis lib command arguments must be strings or integers')
  return decoded
 `
	got, err := ts.client.Eval(ctx, script, []string{key}, binary).Text()
	if err != nil || got != binary {
		t.Fatalf("binary round trip = %q, %v; want %q", got, err, binary)
	}
	stored, err := ts.client.Get(ctx, key).Result()
	if err != nil || stored != `"\u0000\u0001`+string([]byte{127, 128, 254, 255})+`\/<>&"` {
		t.Fatalf("stored value changed or escaped incorrectly: %q, %v", stored, err)
	}
}
