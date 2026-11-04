package handler

import (
	"context"
	"fmt"
	"strings"
	"testing"

	lua "github.com/yuin/gopher-lua"
)

func TestLuaCJSON(t *testing.T) {
	tests := map[string]string{
		"decode nested null and unicode": `
            local v = cjson.decode('{"a":[null,true,3,"\\u263a"],"b":{"x":null}}')
            assert(v.a[1] == cjson.null and v.a[4] == '☺' and v.b.x == cjson.null)
            assert(cjson.encode(v.a) == '[null,true,3,"☺"]')`,
		"scalars and precision": `
            assert(cjson.encode(nil) == 'null' and cjson.encode(false) == 'false')
            assert(cjson.decode('false') == false and cjson.decode('null') == cjson.null)
            assert(cjson.encode(1.234567890123456) == '1.2345678901235')`,
		"table shapes": `
            assert(cjson.encode({}) == '{}' and cjson.encode(cjson.decode('[]')) == '{}')
            assert(cjson.encode({[1]='a',[3]='c'}) == '["a",null,"c"]')
            assert(cjson.encode({[1]='x',a='y'}) == '{"1":"x","a":"y"}')`,
		"invalid JSON": `
            for _, s in ipairs({'[1,]', 'true false', '1e9999', ''}) do
                assert(not pcall(cjson.decode, s))
            end`,
		"invalid values": `
            local cyc = {}; cyc.x = cyc
            local values = {cyc, function() end, 0/0, 1/0, {[100]=1}, {[1e20]=1},
                            {[1/0]=1}, {[1/0]=1, a=2}, {[1]=1, ['1']=2}}
            for i, v in ipairs(values) do
                assert(not pcall(cjson.encode, v), tostring(i))
            end`,
		"shared child and raw access": `
            local child = {x=1}
            assert(pcall(cjson.encode, {a=child,b=child}))
            local t = setmetatable({a=1}, {
                __index=function() error('metamethod') end,
                __pairs=function() error('metamethod') end
            })
            assert(pcall(cjson.encode, t))`,
		"argument count": `
            assert(not pcall(cjson.encode) and not pcall(cjson.decode))
            assert(not pcall(cjson.encode,1,2) and not pcall(cjson.decode,'1',2))
            assert(cjson.encode(nil) == 'null')`,
		"captured null": `
            local n = cjson.null
            cjson.null = 'changed'
            assert(cjson.decode('null') == n and cjson.encode(n) == 'null')`,
	}
	for name, script := range tests {
		t.Run(name, func(t *testing.T) {
			L := lua.NewState(lua.Options{SkipOpenLibs: true})
			defer L.Close()
			openSafeLibs(L)
			if err := L.DoString(script); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLuaCJSONDepth(t *testing.T) {
	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer L.Close()
	openSafeLibs(L)
	for _, depth := range []int{1000, 1001} {
		input := strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth)
		L.SetGlobal("input", lua.LString(input))
		L.SetGlobal("allowed", lua.LBool(depth == 1000))
		if err := L.DoString(`assert(pcall(cjson.decode,input)==allowed); local t=0; for i=1,` + fmt.Sprint(depth) + ` do t={t} end; assert(pcall(cjson.encode,t)==allowed)`); err != nil {
			t.Fatalf("depth %d: %v", depth, err)
		}
	}
}

func TestLuaCJSONStateIsolation(t *testing.T) {
	a := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer a.Close()
	openSafeLibs(a)
	b := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer b.Close()
	openSafeLibs(b)
	if a.GetField(a.GetGlobal("cjson"), "null") == b.GetField(b.GetGlobal("cjson"), "null") {
		t.Fatal("states share null sentinel")
	}
}

func TestLuaCJSONExecutor(t *testing.T) {
	le := newLuaExecutor(context.Background(), nil, nil, nil, nil)
	got, err := le.Execute(`return cjson.encode(cjson.decode('{"answer":42}'))`)
	if err != nil {
		t.Fatal(err)
	}
	if got.Bulk != `{"answer":42}` {
		t.Fatalf("unexpected response: %#v", got)
	}
}

func TestLuaCJSONBinaryStrings(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	openCJSON(L)
	// Cover every byte, including malformed UTF-8 and embedded NUL, as both
	// values and keys. Compare bytes directly, not through a JSON decoder.
	var binary strings.Builder
	for i := 0; i < 256; i++ {
		binary.WriteByte(byte(i))
	}
	L.SetGlobal("binary", lua.LString(binary.String()))
	if err := L.DoString(`
  assert(cjson.decode(cjson.encode(binary)) == binary)
  local object = {}; object[binary] = binary
  assert(cjson.decode(cjson.encode(object))[binary] == binary)
  assert(cjson.decode('"' .. string.char(255,254) .. 'abc"') == string.char(255,254) .. 'abc')
  assert(cjson.encode('<>&/"\\\b\f\n\r\t') == '"<>&\\/\\"\\\\\\b\\f\\n\\r\\t"')
  assert(cjson.decode('"\\uD834\\uDD1E"') == '𝄞')
  for _, s in ipairs({'"\\uD800"','"\\uDC00"','"\\uD800\\u0041"','"\\uD800x"'}) do
   assert(not pcall(cjson.decode,s), s)
  end
  local nested = cjson.decode('{"a:b,c":["x",{"escaped\\"key":"v"}],"last":"z"}')
  assert(nested['a:b,c'][2]['escaped"key'] == 'v' and nested.last == 'z')
 `); err != nil {
		t.Fatal(err)
	}
}

func TestLuaCJSONInvalidRedisArguments(t *testing.T) {
	le := newLuaExecutor(context.Background(), nil, nil, nil, nil)
	got, err := le.Execute(`
  local expected = 'ERR Lua redis lib command arguments must be strings or integers'
  local values = {cjson.null, {}, true, false, function() end}
  for _, v in ipairs(values) do
   assert(redis.pcall('SET', 'key', v).err == expected)
   local ok, msg = pcall(redis.call, 'SET', 'key', v)
   assert(not ok and string.find(msg, expected, 1, true))
   assert(redis.pcall(v).err == expected)
  end
  assert(redis.pcall('SET','key',nil).err == expected)
  return 'ok'
 `)
	if err != nil {
		t.Fatal(err)
	}
	if got.Bulk != "ok" {
		t.Fatalf("unexpected response: %#v", got)
	}
}
