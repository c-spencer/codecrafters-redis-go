package server

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/codecrafters-io/redis-starter-go/internal/protocol"
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
)

type ReplicationState struct {
	role             string
	masterReplid     string
	masterReplOffset int
	connectedSlaves  int
}

func (r *ReplicationState) Get(key string) (string, bool) {
	switch key {
	case "role":
		return r.role, true
	case "masterReplId":
		return r.masterReplid, true
	default:
		return "", false
	}
}

func (r *ReplicationState) GetInt(key string) (int, bool) {
	switch key {
	case "masterReplOffset":
		return r.masterReplOffset, true
	case "connectedSlaves":
		return r.connectedSlaves, true
	default:
		return 0, false
	}
}

// Start the replication goroutine, which will handle all replication tasks.
// This connects to the master and listens for commands, forwarding them to the executor.
func (s *ExecutorState) runReplication(wg *sync.WaitGroup) {
	defer wg.Done()

	parts := strings.Split(s.config.ReplicaOf, " ")

	if len(parts) != 2 {
		s.logger.Fatal().Msgf("[replication] replicaof must be of form '<HOST> <PORT>', got '%s'", s.config.ReplicaOf)
	}

	conn, err := net.Dial("tcp", strings.Join(parts, ":"))

	if err != nil {
		s.logger.Fatal().Msgf("[replication] Got error connecting to master %#v", err)
	}

	reader := bufio.NewReader(conn)

	// Handshake part 1
	// PING PONG

	conn.Write([]byte(protocol.EncodeArray([]string{"PING"})))
	resp, _, _ := protocol.ReadString(reader)

	if resp != "PONG" {
		s.logger.Fatal().Msgf("[replication] Expected PONG in response to PING, got %s", resp)
	}

	// Handshake part 2
	// REPLCONF

	conn.Write([]byte(protocol.EncodeArray([]string{
		"REPLCONF", "listening-port", s.config.Port,
	})))
	resp, _, _ = protocol.ReadString(reader)
	if resp != "OK" {
		s.logger.Fatal().Msgf("[replication] Expected OK in response to REPLCONF, got %s", resp)
	}

	conn.Write([]byte(protocol.EncodeArray([]string{
		"REPLCONF", "capa", "psync2",
	})))
	resp, _, _ = protocol.ReadString(reader)
	if resp != "OK" {
		s.logger.Fatal().Msgf("[replication] Expected OK in response to REPLCONF, got %s", resp)
	}

	// Handshake part 3
	// PSYNC

	conn.Write([]byte(protocol.EncodeArray([]string{"PSYNC", "?", "-1"})))
	resp, _, _ = protocol.ReadString(reader)

	s.logger.Debug().Msgf("[replication] Got PSYNC response: %s", resp)

	dbfile, _ := protocol.ReadBytes(reader)

	s.logger.Info().Msgf("[replication] Synchronised dbfile of length %d from master, hydrating", len(dbfile))

	db, err := rdb.LoadDatabaseFromReader(bytes.NewReader(dbfile))
	if err == nil {
		s.replicaCh <- ReplicationMessage{
			messageType: "db",
			payload:     db.Hashtable,
		}
	} else {
		s.logger.Fatal().Msgf("[replication] Error loading database from master: %s", err)
	}

	connState := Connection{
		// Special ID for the master connection
		id:   -1,
		conn: conn,
		addr: conn.RemoteAddr().String(),

		replOffset: &atomic.Int64{},
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			cmd, bytesRead, err := receiveCommand(conn, reader)

			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					// Timeout is expected, yielding to check for graceful shutdown.
					continue
				} else if err == io.EOF {
					s.logger.Warn().Msg("[replication] Master disconnected, terminating replication.")
					return
				} else {
					s.logger.Fatal().Msgf("[replication] Got error reading from master %#v", err)
				}
			}

			handler, err := cmd.Handler()

			if err != nil {
				s.logger.Error().Msgf("[replication] Got error getting handler for command `%#v` from master", err)
				result := protocol.EncodeError(err.Error())
				conn.Write([]byte(result))
			}

			s.commandCh <- CommandRequest{
				handler:      handler,
				conn:         &connState,
				commandBytes: bytesRead,
			}
		}
	}
}
