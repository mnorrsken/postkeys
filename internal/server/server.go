// Package server provides the TCP server for Redis protocol.
package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/mnorrsken/postkeys/internal/handler"
	"github.com/mnorrsken/postkeys/internal/metrics"
	"github.com/mnorrsken/postkeys/internal/pubsub"
	"github.com/mnorrsken/postkeys/internal/resp"
)

// Server represents a Redis-compatible server
type Server struct {
	addr       string
	handler    *handler.Handler
	listener   net.Listener
	quit       chan struct{}
	wg         sync.WaitGroup
	debug      bool
	traceLevel int // 0=off, 1=important only, 2=most commands, 3=everything
	pubsub     *pubsub.Hub

	// Connection tracking for graceful drain
	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
}

// New creates a new server
func New(addr string, h *handler.Handler) *Server {
	return &Server{
		addr:    addr,
		handler: h,
		quit:    make(chan struct{}),
		debug:   false,
		conns:   make(map[net.Conn]struct{}),
	}
}

// NewWithDebug creates a new server with debug logging
func NewWithDebug(addr string, h *handler.Handler, debug bool) *Server {
	return &Server{
		addr:    addr,
		handler: h,
		quit:    make(chan struct{}),
		debug:   debug,
		conns:   make(map[net.Conn]struct{}),
	}
}

// NewWithOptions creates a new server with all options
func NewWithOptions(addr string, h *handler.Handler, debug bool, traceLevel int) *Server {
	return &Server{
		addr:       addr,
		handler:    h,
		quit:       make(chan struct{}),
		debug:      debug,
		traceLevel: traceLevel,
		conns:      make(map[net.Conn]struct{}),
	}
}

// SetPubSubHub sets the pub/sub hub for the server
func (s *Server) SetPubSubHub(hub *pubsub.Hub) {
	s.pubsub = hub
	if hub != nil {
		hub.SetDebug(s.debug)
	}
}

// Start starts the server
func (s *Server) Start(ctx context.Context) error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	log.Printf("Server listening on %s", s.addr)

	go s.acceptLoop(ctx)

	return nil
}

// ServeWithListener starts the server with an existing listener
func (s *Server) ServeWithListener(listener net.Listener) error {
	s.listener = listener
	log.Printf("Server listening on %s", listener.Addr().String())
	s.acceptLoop(context.Background())
	return nil
}

// Close closes the server (alias for Stop)
func (s *Server) Close() {
	s.Stop()
}

// Stop immediately stops the server (closes listener, stops pub/sub, waits for handlers).
func (s *Server) Stop() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
	if s.pubsub != nil {
		s.pubsub.Stop()
	}
	s.wg.Wait()
}

// CloseListener closes the TCP listener so no new connections are accepted.
// Existing connections remain active until drained.
func (s *Server) CloseListener() {
	if s.listener != nil {
		s.listener.Close()
	}
}

// DrainConnections sets a read deadline on all active connections, stops
// the pub/sub hub, and waits for all connection handlers to finish.
func (s *Server) DrainConnections(timeout time.Duration) {
	if s.pubsub != nil {
		s.pubsub.Stop()
	}

	deadline := time.Now().Add(timeout)
	s.connsMu.Lock()
	for c := range s.conns {
		c.SetReadDeadline(deadline) //nolint:errcheck
	}
	s.connsMu.Unlock()

	s.wg.Wait()
}

func (s *Server) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		metrics.ConnectionsTotal.Inc()
		metrics.ActiveConnections.Inc()
		go s.handleConnection(ctx, conn)
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	defer metrics.ActiveConnections.Dec()

	// Track connection for graceful drain
	s.connsMu.Lock()
	s.conns[conn] = struct{}{}
	s.connsMu.Unlock()
	defer func() {
		s.connsMu.Lock()
		delete(s.conns, conn)
		s.connsMu.Unlock()
	}()

	reader := resp.NewReaderWithDebug(conn, s.debug)
	writer := resp.NewWriter(conn)

	// Create client state for this connection
	client := NewClientState(conn, s.debug)
	client.SetWriter(writer)

	// Clean up subscriptions when connection closes
	defer func() {
		if s.debug {
			log.Printf("[DEBUG] Client %d (%s) connection closing, inPubSubMode=%v", client.GetID(), conn.RemoteAddr(), client.InPubSubMode())
		}
		if s.pubsub != nil {
			s.pubsub.RemoveSubscriber(client.GetID())
		}
	}()

	// Track authentication state for this connection
	authenticated := !s.handler.RequiresAuth()

	for {

		// Read command
		cmd, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				return
			}
			// During graceful shutdown, read deadline timeouts are expected
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return
			}
			if s.debug {
				log.Printf("[DEBUG] Read error from %s: %v", conn.RemoteAddr(), err)
			} else {
				log.Printf("Read error: %v", err)
			}
			return
		}

		// Trace incoming command
		s.traceCommand(conn, cmd)

		// Check authentication before processing commands
		var response resp.Value
		var multiResponse []resp.Value

		// Add protocol version to context for handlers
		cmdCtx := handler.WithProtocolVersion(ctx, client.GetProtocolVersion())

		if cmd.Type == resp.Array && len(cmd.Array) > 0 {
			cmdName := strings.ToUpper(cmd.Array[0].Bulk)

			// Handle AUTH command specially
			if cmdName == "AUTH" {
				response = s.handler.Handle(cmdCtx, cmd)
				if response.Type == resp.SimpleString && response.Str == "OK" {
					authenticated = true
				}
			} else if cmdName == "HELLO" {
				// Check if HELLO contains AUTH credentials
				hasAuth, authSuccess := s.handler.CheckHelloAuth(cmd)
				// Get and set the protocol version from HELLO
				protoVersion := s.handler.GetHelloProtocolVersion(cmd)
				client.SetProtocolVersion(protoVersion)
				// Update cmdCtx with the new protocol version
				cmdCtx = handler.WithProtocolVersion(ctx, protoVersion)
				response = s.handler.Handle(cmdCtx, cmd)
				// If HELLO included AUTH and it succeeded, mark as authenticated
				if hasAuth && authSuccess && response.Type != resp.Error {
					authenticated = true
				}
			} else if !authenticated {
				// Allow only PING, QUIT, COMMAND, and HELLO without auth
				if cmdName == "PING" || cmdName == "QUIT" || cmdName == "COMMAND" || cmdName == "HELLO" {
					response = s.handler.Handle(cmdCtx, cmd)
				} else {
					if s.debug {
						log.Printf("[DEBUG] NOAUTH: client %s attempted %s without authentication", conn.RemoteAddr(), cmdName)
					}
					response = resp.Value{Type: resp.Error, Str: "NOAUTH Authentication required."}
				}
			} else if cmdName == "SUBSCRIBE" {
				// Handle SUBSCRIBE command
				if s.debug {
					log.Printf("[DEBUG] Client %s calling SUBSCRIBE with channels: %v", conn.RemoteAddr(), extractBulkStrings(cmd.Array[1:]))
				}
				if s.pubsub == nil {
					response = resp.Err("pub/sub is not enabled")
				} else {
					channels := extractBulkStrings(cmd.Array[1:])
					multiResponse = s.handler.HandleSubscribe(s.pubsub, client, channels)
				}
			} else if cmdName == "UNSUBSCRIBE" {
				// Handle UNSUBSCRIBE command
				if s.pubsub == nil {
					response = resp.Err("pub/sub is not enabled")
				} else {
					channels := extractBulkStrings(cmd.Array[1:])
					multiResponse = s.handler.HandleUnsubscribe(s.pubsub, client, channels)
				}
			} else if cmdName == "PSUBSCRIBE" {
				// Handle PSUBSCRIBE command
				if s.pubsub == nil {
					response = resp.Err("pub/sub is not enabled")
				} else {
					patterns := extractBulkStrings(cmd.Array[1:])
					multiResponse = s.handler.HandlePSubscribe(s.pubsub, client, patterns)
				}
			} else if cmdName == "PUNSUBSCRIBE" {
				// Handle PUNSUBSCRIBE command
				if s.pubsub == nil {
					response = resp.Err("pub/sub is not enabled")
				} else {
					patterns := extractBulkStrings(cmd.Array[1:])
					multiResponse = s.handler.HandlePUnsubscribe(s.pubsub, client, patterns)
				}
			} else if cmdName == "PUBLISH" {
				// Handle PUBLISH command
				if s.pubsub == nil {
					response = resp.Err("pub/sub is not enabled")
				} else if len(cmd.Array) < 3 {
					response = resp.ErrWrongArgs("publish")
				} else {
					channel := cmd.Array[1].Bulk
					message := cmd.Array[2].Bulk
					response = s.handler.HandlePublish(ctx, s.pubsub, channel, message)
				}
			} else if client.InPubSubMode() && !client.UseRESP3() {
				// In RESP2 pub/sub mode, only allow SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT
				// RESP3 clients can continue to use regular commands while subscribed (using Push type for messages)
				if cmdName == "PING" || cmdName == "QUIT" {
					response = s.handler.Handle(cmdCtx, cmd)
				} else {
					if s.debug {
						log.Printf("[DEBUG] Client %d (%s) blocked command %s - in RESP2 pub/sub mode", client.GetID(), conn.RemoteAddr(), cmdName)
					}
					response = resp.Err("only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context")
				}
			} else if cmdName == "MULTI" {
				// Start a transaction
				response = s.handler.HandleMulti(client)
			} else if cmdName == "EXEC" {
				// Execute queued commands
				response = s.handler.HandleExec(cmdCtx, client)
			} else if cmdName == "DISCARD" {
				// Discard the transaction
				response = s.handler.HandleDiscard(client)
			} else if client.InTransaction() {
				// Queue commands if in transaction mode (except MULTI, EXEC, DISCARD which are handled above)
				client.QueueCommand(cmd)
				response = resp.Value{Type: resp.SimpleString, Str: "QUEUED"}
			} else if cmdName == "CLIENT" {
				// Handle CLIENT commands with client state
				response = s.handler.HandleClient(cmd, client)
			} else {
				response = s.handler.Handle(cmdCtx, cmd)
			}
		} else {
			response = s.handler.Handle(cmdCtx, cmd)
		}

		// Log error responses when debug is enabled
		if s.debug && response.Type == resp.Error {
			cmdName := ""
			if cmd.Type == resp.Array && len(cmd.Array) > 0 {
				cmdName = strings.ToUpper(cmd.Array[0].Bulk)
			}
			log.Printf("[DEBUG] Error response to %s for %s: %s", conn.RemoteAddr(), cmdName, response.Str)
		}

		// Trace outgoing response(s)
		if len(multiResponse) > 0 {
			for _, r := range multiResponse {
				s.traceResponse(conn, cmd, r)
			}
		} else {
			s.traceResponse(conn, cmd, response)
		}

		// Write response(s) - pub/sub commands may have multiple responses
		if len(multiResponse) > 0 {
			for _, r := range multiResponse {
				if err := writer.WriteValue(r); err != nil {
					if s.debug {
						log.Printf("[DEBUG] Write error to %s: %v", conn.RemoteAddr(), err)
					} else {
						log.Printf("Write error: %v", err)
					}
					return
				}
			}
		} else {
			if err := writer.WriteValue(response); err != nil {
				if s.debug {
					log.Printf("[DEBUG] Write error to %s: %v", conn.RemoteAddr(), err)
				} else {
					log.Printf("Write error: %v", err)
				}
				return
			}
		}
		if err := writer.Flush(); err != nil {
			if s.debug {
				log.Printf("[DEBUG] Flush error to %s: %v", conn.RemoteAddr(), err)
			} else {
				log.Printf("Flush error: %v", err)
			}
			return
		}
	}
}

// extractBulkStrings extracts bulk strings from a slice of RESP values
func extractBulkStrings(values []resp.Value) []string {
	result := make([]string, len(values))
	for i, v := range values {
		result[i] = v.Bulk
	}
	return result
}

// formatRESPValue formats a RESP value for trace logging, detecting binary data
func formatRESPValue(v resp.Value) string {
	switch v.Type {
	case resp.Array:
		parts := make([]string, len(v.Array))
		for i, elem := range v.Array {
			parts[i] = formatRESPValue(elem)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case resp.BulkString:
		return formatTraceString(v.Bulk)
	case resp.SimpleString:
		return "+" + v.Str
	case resp.Error:
		return "-" + v.Str
	case resp.Integer:
		return fmt.Sprintf(":%d", v.Num)
	case resp.Null:
		return "(nil)"
	default:
		return formatTraceString(v.Bulk)
	}
}

// formatTraceString formats a string for trace logging, detecting binary data
func formatTraceString(s string) string {
	if isBinaryString(s) {
		return fmt.Sprintf("<binary:%s>", formatTraceSize(len(s)))
	}
	if len(s) > 100 {
		return fmt.Sprintf("\"%s...\" (%s)", s[:100], formatTraceSize(len(s)))
	}
	return fmt.Sprintf("\"%s\"", s)
}

// isBinaryString checks if a string contains binary data
func isBinaryString(s string) bool {
	if !utf8.ValidString(s) {
		return true
	}
	for _, r := range s {
		if r == utf8.RuneError {
			return true
		}
		// Check for control characters except common whitespace
		if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
			return true
		}
	}
	return false
}

// formatTraceSize formats a size for trace logging
func formatTraceSize(size int) string {
	if size < 1024 {
		return fmt.Sprintf("%dB", size)
	}
	if size < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(size)/1024)
	}
	return fmt.Sprintf("%.1fMB", float64(size)/(1024*1024))
}

// RESP command trace levels:
// Level 1: Important/infrequent commands (AUTH, FLUSHDB, FLUSHALL, SHUTDOWN, CONFIG, DEBUG, CLUSTER)
// Level 2: Most commands except high-frequency ones (excludes GET, SET, HGET, HSET, LPUSH, RPUSH, etc.)
// Level 3: Everything including GET, SET, and other high-frequency commands

// getCommandLevel returns the minimum trace level required to log a command
func getCommandLevel(cmdName string) int {
	switch cmdName {
	// Level 1: Important/administrative commands
	case "AUTH", "FLUSHDB", "FLUSHALL", "SHUTDOWN", "DEBUG", "CONFIG", "CLUSTER",
		"BGREWRITEAOF", "BGSAVE", "SAVE", "SLAVEOF", "REPLICAOF", "FAILOVER",
		"ACL", "SLOWLOG", "MIGRATE", "RESTORE", "DUMP":
		return 1

	// Level 3: High-frequency commands (most common operations)
	case "GET", "SET", "MGET", "MSET", "SETEX", "SETNX", "GETSET",
		"HGET", "HSET", "HMGET", "HMSET", "HGETALL", "HDEL", "HEXISTS", "HLEN", "HKEYS", "HVALS",
		"LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE", "LLEN", "LINDEX", "LSET",
		"SADD", "SREM", "SMEMBERS", "SISMEMBER", "SCARD", "SPOP", "SRANDMEMBER",
		"ZADD", "ZREM", "ZRANGE", "ZRANGEBYSCORE", "ZRANK", "ZSCORE", "ZCARD", "ZINCRBY",
		"INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT",
		"EXPIRE", "TTL", "PTTL", "EXPIREAT", "PEXPIRE", "PEXPIREAT", "PERSIST",
		"EXISTS", "DEL", "TYPE", "KEYS", "SCAN", "HSCAN", "SSCAN", "ZSCAN",
		"PING", "ECHO", "TIME", "DBSIZE",
		"PFADD", "PFCOUNT", "PFMERGE":
		return 3

	// Level 2: Everything else (moderate frequency)
	default:
		return 2
	}
}

// traceCommand logs a RESP command for tracing based on trace level
func (s *Server) traceCommand(conn net.Conn, cmd resp.Value) {
	if s.traceLevel <= 0 {
		return
	}

	// Extract command name for level check
	cmdName := ""
	if cmd.Type == resp.Array && len(cmd.Array) > 0 {
		cmdName = strings.ToUpper(cmd.Array[0].Bulk)
	}

	if s.traceLevel >= getCommandLevel(cmdName) {
		log.Printf("[TRACE] %s <- %s", conn.RemoteAddr(), formatRESPValue(cmd))
	}
}

// traceResponse logs a RESP response for tracing based on trace level
func (s *Server) traceResponse(conn net.Conn, cmd resp.Value, response resp.Value) {
	if s.traceLevel <= 0 {
		return
	}

	// Extract command name for level check
	cmdName := ""
	if cmd.Type == resp.Array && len(cmd.Array) > 0 {
		cmdName = strings.ToUpper(cmd.Array[0].Bulk)
	}

	// Always log errors regardless of level (if tracing is on)
	isError := response.Type == resp.Error
	if isError || s.traceLevel >= getCommandLevel(cmdName) {
		log.Printf("[TRACE] %s -> %s", conn.RemoteAddr(), formatRESPValue(response))
	}
}
