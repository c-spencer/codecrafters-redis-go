package server

import (
	"net"
	"sync/atomic"

	"github.com/codecrafters-io/redis-starter-go/internal/commands"
)

// A Connection represents a connection between client and server.
type Connection struct {
	// Id will be a positive integer for normal client to server connections.
	// For the special case of connection from server to master, id will be -1.
	id   int
	conn net.Conn
	addr string

	// Buffering state for MULTI/EXEC transactions. This is done at the connection level,
	// in order to isolate and remove the need for the executor goroutine to handle this state.
	isBuffering bool
	buffer      []commands.Handler
	bufferBytes int

	// Replication offset for this connection. This is used to track the latest offset that
	// the client has issued commands at, in order to know which offset to WAIT for on replicas.
	//
	// This field is dual purpose, as when Connection represents a replica link, it is used to
	// store the latest offset that the replica has ack'd to the master.
	replOffset *atomic.Int64

	// Whether this connection is a replica link (a connection from a replica to the master)
	isReplica bool
}

// Clear the buffer of commands for this connection, resetting the state to non-buffering.
func (c *Connection) clearBuffer() {
	c.buffer = []commands.Handler{}
	c.isBuffering = false
	c.bufferBytes = 0
}
