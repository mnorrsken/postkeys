package server

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mnorrsken/postkeys/internal/resp"
)

var clientIDCounter uint64

// ClientState holds per-connection state
type ClientState struct {
	ID         uint64
	Name       string
	Addr       string
	CreatedAt  time.Time
	LibName    string
	LibVersion string
	mu         sync.RWMutex
	debug      bool

	// Protocol version (2 or 3, defaults to 2 for RESP2)
	protocolVersion int

	// Transaction state
	inTransaction  bool
	queuedCommands []resp.Value

	// Pub/sub state
	inPubSubMode bool
	writer       *resp.Writer
	writerMu     sync.Mutex
}

// NewClientState creates a new client state for a connection
func NewClientState(conn net.Conn, debug bool) *ClientState {
	id := atomic.AddUint64(&clientIDCounter, 1)
	addr := conn.RemoteAddr().String()
	if debug {
		log.Printf("[DEBUG] New client %d created from %s", id, addr)
	}
	return &ClientState{
		ID:              id,
		Addr:            addr,
		CreatedAt:       time.Now(),
		protocolVersion: 2, // Default to RESP2
		debug:           debug,
	}
}

// GetID returns the client ID
func (c *ClientState) GetID() uint64 {
	return c.ID
}

// GetProtocolVersion returns the RESP protocol version (2 or 3)
func (c *ClientState) GetProtocolVersion() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.protocolVersion
}

// SetProtocolVersion sets the RESP protocol version
func (c *ClientState) SetProtocolVersion(version int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if version >= 2 && version <= 3 {
		c.protocolVersion = version
	}
}

// UseRESP3 returns true if the client requested RESP3
func (c *ClientState) UseRESP3() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.protocolVersion >= 3
}

// GetName returns the client name
func (c *ClientState) GetName() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Name
}

// SetName sets the client name
func (c *ClientState) SetName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Name = name
}

// SetLibInfo sets the client library info
func (c *ClientState) SetLibInfo(libName, libVersion string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if libName != "" {
		c.LibName = libName
	}
	if libVersion != "" {
		c.LibVersion = libVersion
	}
}

// GetInfo returns a formatted info string for CLIENT INFO/LIST
func (c *ClientState) GetInfo() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	age := int64(time.Since(c.CreatedAt).Seconds())

	info := fmt.Sprintf("id=%d addr=%s age=%d name=%s",
		c.ID, c.Addr, age, c.Name)

	if c.LibName != "" {
		info += fmt.Sprintf(" lib-name=%s", c.LibName)
	}
	if c.LibVersion != "" {
		info += fmt.Sprintf(" lib-ver=%s", c.LibVersion)
	}

	return info
}

// InTransaction returns true if the client is in a MULTI transaction
func (c *ClientState) InTransaction() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.inTransaction
}

// StartTransaction starts a MULTI transaction
func (c *ClientState) StartTransaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inTransaction {
		return fmt.Errorf("ERR MULTI calls can not be nested")
	}
	c.inTransaction = true
	c.queuedCommands = nil
	return nil
}

// QueueCommand adds a command to the transaction queue
func (c *ClientState) QueueCommand(cmd resp.Value) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queuedCommands = append(c.queuedCommands, cmd)
}

// GetQueuedCommands returns the queued commands and clears them
func (c *ClientState) GetQueuedCommands() []resp.Value {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmds := c.queuedCommands
	c.queuedCommands = nil
	c.inTransaction = false
	return cmds
}

// DiscardTransaction discards the current transaction
func (c *ClientState) DiscardTransaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.inTransaction {
		return fmt.Errorf("ERR DISCARD without MULTI")
	}
	c.inTransaction = false
	c.queuedCommands = nil
	return nil
}

// QueueLength returns the number of queued commands
func (c *ClientState) QueueLength() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.queuedCommands)
}

// SetWriter sets the RESP writer for pub/sub message delivery
func (c *ClientState) SetWriter(w *resp.Writer) {
	c.writerMu.Lock()
	defer c.writerMu.Unlock()
	c.writer = w
}

// EnterPubSubMode marks the client as in pub/sub mode
func (c *ClientState) EnterPubSubMode() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inPubSubMode = true
}

// ExitPubSubMode marks the client as no longer in pub/sub mode
func (c *ClientState) ExitPubSubMode() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inPubSubMode = false
}

// InPubSubMode returns true if the client is in pub/sub mode
func (c *ClientState) InPubSubMode() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.inPubSubMode
}

// WriteResponses writes one or more RESP values to the client and flushes,
// holding the writer lock so command responses cannot interleave with pub/sub
// messages delivered from the hub goroutine.
func (c *ClientState) WriteResponses(values ...resp.Value) error {
	c.writerMu.Lock()
	defer c.writerMu.Unlock()

	if c.writer == nil {
		return fmt.Errorf("no writer set for client")
	}

	for _, v := range values {
		if err := c.writer.WriteValue(v); err != nil {
			return err
		}
	}
	return c.writer.Flush()
}

// SendPubSubMessage sends a pub/sub message to the client
// This implements the pubsub.Subscriber interface
func (c *ClientState) SendPubSubMessage(msgType, channel, payload string) error {
	c.writerMu.Lock()
	defer c.writerMu.Unlock()

	if c.writer == nil {
		return fmt.Errorf("no writer set for client")
	}

	var response resp.Value

	// RESP3 clients receive pub/sub messages as Push type (>)
	// RESP2 clients receive them as regular arrays
	respType := resp.Array
	if c.UseRESP3() {
		respType = resp.Push
	}

	switch msgType {
	case "message":
		response = resp.Value{
			Type: respType,
			Array: []resp.Value{
				resp.Bulk("message"),
				resp.Bulk(channel),
				resp.Bulk(payload),
			},
		}
	case "pmessage":
		// channel is "pattern\x00actualChannel" for pmessage
		parts := strings.SplitN(channel, "\x00", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid pmessage channel format")
		}
		response = resp.Value{
			Type: respType,
			Array: []resp.Value{
				resp.Bulk("pmessage"),
				resp.Bulk(parts[0]), // pattern
				resp.Bulk(parts[1]), // channel
				resp.Bulk(payload),
			},
		}
	default:
		return fmt.Errorf("unknown message type: %s", msgType)
	}

	if err := c.writer.WriteValue(response); err != nil {
		return err
	}
	return c.writer.Flush()
}
