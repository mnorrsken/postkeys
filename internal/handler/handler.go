// Package handler implements Redis command handlers.
package handler

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/mnorrsken/postkeys/internal/metrics"
	"github.com/mnorrsken/postkeys/internal/resp"
	"github.com/mnorrsken/postkeys/internal/storage"
)

// Context key for protocol version
type contextKey string

const protocolVersionKey contextKey = "protocolVersion"

// WithProtocolVersion adds the protocol version to the context
func WithProtocolVersion(ctx context.Context, version int) context.Context {
	return context.WithValue(ctx, protocolVersionKey, version)
}

// GetProtocolVersion gets the protocol version from context (defaults to 2)
func GetProtocolVersion(ctx context.Context) int {
	if v, ok := ctx.Value(protocolVersionKey).(int); ok {
		return v
	}
	return 2
}

// UseRESP3 returns true if the context indicates RESP3 protocol
func UseRESP3(ctx context.Context) bool {
	return GetProtocolVersion(ctx) >= 3
}

// ClientState interface for client connection state
type ClientState interface {
	GetID() uint64
	GetName() string
	SetName(name string)
	SetLibInfo(libName, libVersion string)
	GetInfo() string
}

// TransactionClientState interface for transaction-capable client state
type TransactionClientState interface {
	ClientState
	InTransaction() bool
	StartTransaction() error
	QueueCommand(cmd resp.Value)
	GetQueuedCommands() []resp.Value
	DiscardTransaction() error
	QueueLength() int
}

// ListNotifier interface for notifying about list push operations
type ListNotifier interface {
	NotifyPush(ctx context.Context, key string) error
	WaitForKey(ctx context.Context, key string, timeout time.Duration) bool
	WaitForKeys(ctx context.Context, keys []string, timeout time.Duration) string
}

// defaultBlockingPollInterval is the fallback poll interval for BLPOP/BRPOP
// when no LISTEN/NOTIFY notifier is configured.
const defaultBlockingPollInterval = 100 * time.Millisecond

// Handler processes Redis commands
type Handler struct {
	store                storage.Backend
	password             string
	startTime            time.Time
	listNotifier         ListNotifier
	blockingPollInterval time.Duration
}

// New creates a new command handler
func New(store storage.Backend, password string) *Handler {
	return &Handler{
		store:                store,
		password:             password,
		startTime:            time.Now(),
		blockingPollInterval: defaultBlockingPollInterval,
	}
}

// SetBlockingPollInterval configures the BLPOP/BRPOP poll fallback interval.
// Values <= 0 are ignored (default is kept).
func (h *Handler) SetBlockingPollInterval(d time.Duration) {
	if d > 0 {
		h.blockingPollInterval = d
	}
}

// SetListNotifier sets the list notifier for BRPOP/BLPOP
func (h *Handler) SetListNotifier(n ListNotifier) {
	h.listNotifier = n
}

// RequiresAuth returns true if a password is configured
func (h *Handler) RequiresAuth() bool {
	return h.password != ""
}

// CheckAuth verifies the provided password
func (h *Handler) CheckAuth(providedPassword string) bool {
	return h.password == providedPassword
}

// GetHelloProtocolVersion extracts the protocol version from a HELLO command
// Returns the requested protocol version (2 or 3), defaults to 2 if not specified
func (h *Handler) GetHelloProtocolVersion(cmd resp.Value) int {
	if cmd.Type != resp.Array || len(cmd.Array) < 2 {
		return 2
	}

	args := cmd.Array[1:]
	if len(args) > 0 {
		if v, err := strconv.Atoi(args[0].Bulk); err == nil && v >= 2 && v <= 3 {
			return v
		}
	}
	return 2
}

// CheckHelloAuth checks if HELLO command contains valid AUTH credentials
// Returns (hasAuth, authSuccess) - hasAuth is true if AUTH was specified, authSuccess is true if auth succeeded
func (h *Handler) CheckHelloAuth(cmd resp.Value) (bool, bool) {
	if cmd.Type != resp.Array || len(cmd.Array) < 2 {
		return false, false
	}

	args := cmd.Array[1:]

	// Skip protocol version if present
	i := 0
	if len(args) > 0 {
		if _, err := strconv.Atoi(args[0].Bulk); err == nil {
			i = 1
		}
	}

	// Look for AUTH option
	for i < len(args) {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "AUTH":
			if i+2 >= len(args) {
				return true, false
			}
			password := args[i+2].Bulk
			return true, h.CheckAuth(password)
		case "SETNAME":
			if i+1 >= len(args) {
				return false, false
			}
			i += 2
		default:
			return false, false
		}
	}

	return false, false
}

// Handle processes a RESP command and returns a response
func (h *Handler) Handle(ctx context.Context, cmd resp.Value) resp.Value {
	if cmd.Type != resp.Array || len(cmd.Array) == 0 {
		return resp.Err("invalid command format")
	}

	// Extract command name
	cmdName := strings.ToUpper(cmd.Array[0].Bulk)
	args := cmd.Array[1:]

	// Record metrics
	start := time.Now()
	result := h.executeCommand(ctx, cmdName, args)
	duration := time.Since(start)
	isError := result.Type == resp.Error
	metrics.RecordCommand(cmdName, duration, isError)

	return result
}

// executeCommand routes and executes the command
func (h *Handler) executeCommand(ctx context.Context, cmdName string, args []resp.Value) resp.Value {
	switch cmdName {
	// Connection commands (stateless, don't use storage)
	case "PING":
		return h.ping(args)
	case "ECHO":
		return h.echo(args)
	case "QUIT":
		return resp.OK()
	case "AUTH":
		return h.auth(args)
	case "HELLO":
		return h.hello(args)
	case "COMMAND":
		return h.command(args)
	case "CLUSTER":
		return h.cluster(args)
	case "CONFIG":
		return h.config(args)
	case "SELECT":
		return h.selectDB(args)
	case "WAIT":
		return h.wait(args)

	// Transaction commands (handled specially in HandleTransaction)
	case "MULTI", "EXEC", "DISCARD":
		return resp.Err("ERR " + cmdName + " must be handled with transaction context")

	// FLUSHDB/FLUSHALL - only available on Backend, not in transactions
	case "FLUSHDB", "FLUSHALL":
		return h.flushdb(ctx, args)

	// All other commands use the unified Operations interface
	default:
		return h.ExecuteWithOps(ctx, h.store, cmdName, args)
	}
}

// HandleClient processes CLIENT commands with connection state
func (h *Handler) HandleClient(cmd resp.Value, client ClientState) resp.Value {
	if cmd.Type != resp.Array || len(cmd.Array) < 2 {
		return resp.ErrWrongArgs("client")
	}

	subCmd := strings.ToUpper(cmd.Array[1].Bulk)
	args := cmd.Array[2:]

	// Record metrics
	start := time.Now()
	result := h.executeClientCommand(subCmd, args, client)
	duration := time.Since(start)
	isError := result.Type == resp.Error
	metrics.RecordCommand("CLIENT", duration, isError)

	return result
}

func (h *Handler) executeClientCommand(subCmd string, args []resp.Value, client ClientState) resp.Value {
	switch subCmd {
	case "ID":
		return resp.Int(int64(client.GetID()))

	case "GETNAME":
		name := client.GetName()
		if name == "" {
			return resp.NullBulk()
		}
		return resp.Bulk(name)

	case "SETNAME":
		if len(args) != 1 {
			return resp.ErrWrongArgs("client setname")
		}
		client.SetName(args[0].Bulk)
		return resp.OK()

	case "SETINFO":
		if len(args) < 2 {
			return resp.ErrWrongArgs("client setinfo")
		}
		infoType := strings.ToUpper(args[0].Bulk)
		infoValue := args[1].Bulk
		switch infoType {
		case "LIB-NAME":
			client.SetLibInfo(infoValue, "")
		case "LIB-VER":
			client.SetLibInfo("", infoValue)
		default:
			return resp.Err("ERR Unknown argument for CLIENT SETINFO")
		}
		return resp.OK()

	case "INFO":
		return resp.Bulk(client.GetInfo())

	case "LIST":
		return resp.Bulk(client.GetInfo() + "\n")

	case "TRACKINGINFO":
		return resp.Arr()

	case "CACHING":
		return resp.OK()

	case "GETREDIR":
		return resp.Int(-1)

	case "UNPAUSE", "PAUSE":
		return resp.OK()

	case "NO-EVICT", "NO-TOUCH":
		return resp.OK()

	case "REPLY":
		if len(args) != 1 {
			return resp.ErrWrongArgs("client reply")
		}
		return resp.OK()

	default:
		return resp.Err(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'", subCmd))
	}
}

// HandleMulti handles the MULTI command to start a transaction
func (h *Handler) HandleMulti(client TransactionClientState) resp.Value {
	if err := client.StartTransaction(); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

// HandleDiscard handles the DISCARD command to abort a transaction
func (h *Handler) HandleDiscard(client TransactionClientState) resp.Value {
	if err := client.DiscardTransaction(); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

const maxExecRetries = 3

// HandleExec executes all queued commands in a transaction, retrying on deadlock.
func (h *Handler) HandleExec(ctx context.Context, client TransactionClientState) resp.Value {
	if !client.InTransaction() {
		return resp.Err("ERR EXEC without MULTI")
	}

	commands := client.GetQueuedCommands()

	for attempt := 0; attempt <= maxExecRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter before retry
			base := time.Duration(5<<uint(attempt-1)) * time.Millisecond
			jitter := time.Duration(rand.Int63n(int64(base)))
			select {
			case <-ctx.Done():
				return resp.Err(fmt.Sprintf("ERR transaction aborted: %v", ctx.Err()))
			case <-time.After(base + jitter):
			}
		}

		// Start a storage transaction
		tx, err := h.store.BeginTx(ctx)
		if err != nil {
			return resp.Err(fmt.Sprintf("ERR transaction start failed: %v", err))
		}

		// Execute all commands within the transaction using the unified Operations interface
		results := make([]resp.Value, len(commands))

		for i, cmd := range commands {
			if cmd.Type != resp.Array || len(cmd.Array) == 0 {
				results[i] = resp.Err("invalid command format")
				continue
			}

			cmdName := strings.ToUpper(cmd.Array[0].Bulk)
			args := cmd.Array[1:]

			// Execute using the unified handler with the transaction
			results[i] = h.ExecuteWithOps(ctx, tx, cmdName, args)
		}

		// Commit the transaction
		if err := tx.Commit(ctx); err != nil {
			_ = tx.Rollback(ctx)
			if storage.IsRetryableError(err) && attempt < maxExecRetries {
				continue
			}
			return resp.Err(fmt.Sprintf("ERR transaction commit failed: %v", err))
		}

		// Record metrics for EXEC
		metrics.RecordCommand("EXEC", 0, false)

		return resp.Value{Type: resp.Array, Array: results}
	}

	return resp.Err("ERR transaction failed after retries")
}

// ============== Connection Commands ==============

func (h *Handler) ping(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{Type: resp.SimpleString, Str: "PONG"}
	}
	return resp.Bulk(args[0].Bulk)
}

func (h *Handler) echo(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("echo")
	}
	return resp.Bulk(args[0].Bulk)
}

func (h *Handler) command(args []resp.Value) resp.Value {
	if len(args) > 0 && strings.ToUpper(args[0].Bulk) == "DOCS" {
		return resp.Arr()
	}
	return resp.Arr()
}

// cluster handles CLUSTER commands - this is a standalone server, not a cluster
func (h *Handler) cluster(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("cluster")
	}

	subCmd := strings.ToUpper(args[0].Bulk)
	switch subCmd {
	case "INFO":
		// Return cluster info indicating cluster mode is disabled (standalone)
		return resp.Bulk("cluster_enabled:0\r\ncluster_state:ok\r\ncluster_slots_assigned:0\r\ncluster_slots_ok:0\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:1\r\ncluster_size:0\r\ncluster_current_epoch:0\r\ncluster_my_epoch:0\r\ncluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\ncluster_stats_messages_ping_sent:0\r\ncluster_stats_messages_pong_sent:0\r\ncluster_stats_messages_meet_sent:0\r\ncluster_stats_messages_ping_received:0\r\ncluster_stats_messages_pong_received:0\r\ncluster_stats_messages_meet_received:0")
	case "SLOTS":
		// Return empty array - no cluster slots configured
		return resp.Arr()
	case "NODES":
		// Return empty response - no cluster nodes
		return resp.Bulk("")
	case "MYID":
		// Return a placeholder node ID
		return resp.Bulk("0000000000000000000000000000000000000000")
	case "KEYSLOT":
		// Return slot 0 for any key (not actually used in standalone mode)
		return resp.Int(0)
	default:
		return resp.Err(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'", subCmd))
	}
}

// selectDB handles SELECT command. postkeys only supports a single database (db 0).
func (h *Handler) selectDB(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("select")
	}
	db, err := strconv.Atoi(args[0].Bulk)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	if db != 0 {
		return resp.Err("ERR DB index is out of range")
	}
	return resp.OK()
}

// wait handles WAIT command. In postkeys, all writes go directly to PostgreSQL
// and are immediately durable, so WAIT always returns immediately.
func (h *Handler) wait(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("wait")
	}
	// numreplicas is args[0], timeout is args[1]
	// Since postkeys has no replicas (PostgreSQL handles durability),
	// return 0 replicas immediately.
	return resp.Int(0)
}

// config handles CONFIG commands. postkeys is not configurable at runtime,
// but many Redis clients (including go-redis used by Authelia) send CONFIG
// commands during initialization. Return sensible defaults to avoid errors.
func (h *Handler) config(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("config")
	}

	subCmd := strings.ToUpper(args[0].Bulk)
	switch subCmd {
	case "GET":
		if len(args) < 2 {
			return resp.ErrWrongArgs("config get")
		}
		// Return empty array for unknown config keys (valid Redis behavior)
		return resp.Arr()
	case "SET":
		// Accept but ignore CONFIG SET (not applicable for postkeys)
		return resp.OK()
	case "RESETSTAT":
		return resp.OK()
	case "REWRITE":
		return resp.OK()
	default:
		return resp.Err(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for 'CONFIG %s'", subCmd))
	}
}

func (h *Handler) hello(args []resp.Value) resp.Value {
	// Default to RESP2, but we now support RESP3 as well
	protover := 2

	if len(args) > 0 {
		// Parse requested protocol version
		if v, err := strconv.Atoi(args[0].Bulk); err == nil {
			if v < 2 || v > 3 {
				return resp.Err("NOPROTO unsupported protocol version")
			}
			// Accept the requested version (we now support both 2 and 3)
			protover = v
		}

		i := 1
		for i < len(args) {
			opt := strings.ToUpper(args[i].Bulk)
			switch opt {
			case "AUTH":
				if i+2 >= len(args) {
					return resp.ErrWrongArgs("hello")
				}
				password := args[i+2].Bulk
				if h.RequiresAuth() && !h.CheckAuth(password) {
					return resp.Value{Type: resp.Error, Str: "WRONGPASS invalid username-password pair"}
				}
				i += 3
			case "SETNAME":
				if i+1 >= len(args) {
					return resp.ErrWrongArgs("hello")
				}
				i += 2
			default:
				return resp.Err(fmt.Sprintf("ERR Unknown option '%s' for HELLO", args[i].Bulk))
			}
		}
	}

	return resp.Arr(
		resp.Bulk("server"),
		resp.Bulk("postkeys"),
		resp.Bulk("version"),
		resp.Bulk("1.0.0"),
		resp.Bulk("proto"),
		resp.Int(int64(protover)),
		resp.Bulk("id"),
		resp.Int(0),
		resp.Bulk("mode"),
		resp.Bulk("standalone"),
		resp.Bulk("role"),
		resp.Bulk("master"),
		resp.Bulk("modules"),
		resp.Arr(),
	)
}

func (h *Handler) auth(args []resp.Value) resp.Value {
	if !h.RequiresAuth() {
		return resp.Err("AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	}
	if len(args) == 0 {
		return resp.ErrWrongArgs("auth")
	}
	var password string
	if len(args) == 1 {
		password = args[0].Bulk
	} else if len(args) == 2 {
		password = args[1].Bulk
	} else {
		return resp.ErrWrongArgs("auth")
	}
	if h.CheckAuth(password) {
		return resp.OK()
	}
	return resp.Value{Type: resp.Error, Str: "WRONGPASS invalid username-password pair"}
}

// ============== Server Commands (Backend-only) ==============

func (h *Handler) flushdb(ctx context.Context, args []resp.Value) resp.Value {
	if err := h.store.FlushDB(ctx); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}
