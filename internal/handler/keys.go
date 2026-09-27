package handler

import (
	"strconv"

	"github.com/mnorrsken/postkeys/internal/resp"
)

// commandKeys returns the keys a command touches, used to lock them up front
// in MULTI/EXEC. Commands without keys or not listed here return nil; missing
// a key only loses the up-front ordering (deadlocks are still retried).
func commandKeys(cmdName string, args []resp.Value) []string {
	bulk := func(vs []resp.Value) []string {
		out := make([]string, len(vs))
		for i, v := range vs {
			out[i] = v.Bulk
		}
		return out
	}
	// numkeysAt reads "numkeys key [key ...]" starting at args[i].
	numkeysAt := func(i int) []string {
		if len(args) <= i {
			return nil
		}
		n, err := strconv.Atoi(args[i].Bulk)
		if err != nil || n < 0 || len(args) < i+1+n {
			return nil
		}
		return bulk(args[i+1 : i+1+n])
	}

	switch cmdName {
	case "PING", "ECHO", "KEYS", "SCAN", "RANDOMKEY", "INFO", "DBSIZE", "SCRIPT",
		"WATCH", "UNWATCH", "OBJECT":
		return nil

	case "DEL", "UNLINK", "EXISTS", "MGET", "SINTER", "SUNION", "SDIFF",
		"SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE", "PFCOUNT", "PFMERGE":
		return bulk(args)

	case "MSET":
		keys := make([]string, 0, len(args)/2)
		for i := 0; i < len(args); i += 2 {
			keys = append(keys, args[i].Bulk)
		}
		return keys

	case "RENAME", "COPY", "RPOPLPUSH", "ZRANGESTORE":
		if len(args) < 2 {
			return nil
		}
		return bulk(args[:2])

	case "BLPOP", "BRPOP":
		if len(args) < 1 {
			return nil
		}
		return bulk(args[:len(args)-1])

	case "LMPOP", "ZMPOP", "SINTERCARD":
		return numkeysAt(0)

	case "BLMPOP", "BZMPOP":
		return numkeysAt(1)

	case "ZUNIONSTORE", "ZINTERSTORE":
		if len(args) < 1 {
			return nil
		}
		return append([]string{args[0].Bulk}, numkeysAt(1)...)

	case "BITOP":
		if len(args) < 2 {
			return nil
		}
		return bulk(args[1:])

	case "EVAL", "EVALSHA":
		return numkeysAt(1)

	default:
		// Every other supported command takes a single key first.
		if len(args) < 1 {
			return nil
		}
		return []string{args[0].Bulk}
	}
}
