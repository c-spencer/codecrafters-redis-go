package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/commands"
	"github.com/codecrafters-io/redis-starter-go/internal/domain"
	"github.com/codecrafters-io/redis-starter-go/internal/protocol"
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
)

type Server struct {
	// The main Values map, holding all keyed values
	values map[string]*rdb.ValueEntry

	config      Config
	replication ReplicationState
	replicas    map[int]*Connection

	ctx context.Context

	commandCh chan CommandRequest

	// Channel for sending replication related data to the main executor.
	// Hacky, but it works for now.
	replicaCh chan ReplicationMessage

	subscriptions []Subscription
}

type ReplicationMessage struct {
	messageType string
	payload     interface{}
}

type Subscription struct {
	keys      []string
	expiresAt time.Time
	callback  func(*rdb.ValueEntry)
}

// Implement domain.State interface

func (s *Server) Get(key string) (*rdb.ValueEntry, bool) {
	value, exists := s.values[key]

	// If the key exists but has expired, delete it and treat it as non-existent
	if exists && value.Expiry != nil && value.Expiry.Before(time.Now()) {
		delete(s.values, key)
		return nil, false
	}

	return value, exists
}
func (s *Server) Set(value *rdb.ValueEntry) {
	s.values[value.Key] = value

	// Check if any subscriptions are interested in this key
	// Placeholder (but obviously correct) code. This should be optimized.
	for i := 0; i < len(s.subscriptions); i++ {
		sub := s.subscriptions[i]

		// Remove the subscription if it has expired
		if !sub.expiresAt.IsZero() && sub.expiresAt.Before(time.Now()) {
			sub.callback(nil)
			s.subscriptions = append(s.subscriptions[:i], s.subscriptions[i+1:]...)
		}

		if slices.Contains(sub.keys, value.Key) {
			sub.callback(value)
			s.subscriptions = append(s.subscriptions[:i], s.subscriptions[i+1:]...)
		}
	}
}
func (s *Server) Delete(key string) {
	delete(s.values, key)
}
func (s *Server) Config() domain.ROMap {
	return &s.config
}
func (s *Server) Keys(pattern string) []string {
	keys := []string{}
	for key := range s.values {
		if matched, _ := path.Match(pattern, key); matched {
			keys = append(keys, key)
		}
	}
	return keys
}
func (s *Server) Subscribe(keys []string, timeout int, callback func(*rdb.ValueEntry)) {
	// Zero time is a special case, meaning "never expire"
	expiresAt := time.Time{}
	if timeout > 0 {
		expiresAt = time.Now().Add(time.Duration(timeout) * time.Millisecond)
	}

	s.subscriptions = append(s.subscriptions, Subscription{
		keys:      keys,
		expiresAt: expiresAt,
		callback:  callback,
	})
}
func (s *Server) ReplicationInfo() domain.ROMap {
	return &s.replication
}

func NewServer(context context.Context, config Config) (*Server, error) {
	replicationState := ReplicationState{
		role:             "master",
		masterReplid:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		masterReplOffset: 0,
	}

	if config.ReplicaOf != "" {
		replicationState.role = "slave"
		replicationState.masterReplid = "?"
	}

	server := Server{
		values:        make(map[string]*rdb.ValueEntry),
		config:        config,
		replication:   replicationState,
		replicas:      make(map[int]*Connection),
		ctx:           context,
		commandCh:     make(chan CommandRequest),
		replicaCh:     make(chan ReplicationMessage),
		subscriptions: []Subscription{},
	}

	db, err := rdb.LoadDatabase(path.Join(config.Dir, config.DBFilename))
	if err == nil {
		server.values = db.Hashtable
	}

	return &server, nil
}

type CommandRequest struct {
	handler    commands.Handler
	connection *Connection
}

func (s *Server) startExecutor() {
	// Run book-keeping tasks every 100ms
	ticker := time.NewTicker(100 * time.Millisecond)

	for {
		select {
		// Graceful shutdown
		case <-s.ctx.Done():
			log.Printf("Shutting down command executor")
			return

		// Regular book-keeping tasks
		case <-ticker.C:
			// Check if any subscriptions are expired.
			// Placeholder (but obviously correct) code. This should be optimized.
			for i := 0; i < len(s.subscriptions); i++ {
				sub := s.subscriptions[i]

				// Remove the subscription if it has expired
				if !sub.expiresAt.IsZero() && sub.expiresAt.Before(time.Now()) {
					sub.callback(nil)
					s.subscriptions = append(s.subscriptions[:i], s.subscriptions[i+1:]...)
				}
			}

		// Handle replication
		case msg := <-s.replicaCh:
			// TODO: Review what other state needs to be updated
			switch msg.messageType {
			case "db":
				s.values = msg.payload.(map[string]*rdb.ValueEntry)
			case "replica":
				replica := msg.payload.(Connection)
				s.replicas[replica.id] = &replica
			}

		// Execute commands from clients
		case req := <-s.commandCh:
			// Forward the command to all replicas if it's a write command
			if s.replication.role == "master" && len(s.replicas) > 0 && req.handler.Mutability()&commands.CmdWrite != 0 {
				cmd := req.handler.Command()
				cmdBytes := []byte(protocol.EncodeArray(append([]string{cmd.Name}, cmd.Arguments...)))

				for _, replica := range s.replicas {
					replica.conn.Write(cmdBytes)
				}
			}

			err := req.handler.Execute(s, func(result string) {
				// Connection can be nil for commands sent via replication
				if req.connection != nil {
					req.connection.conn.Write([]byte(result))
				}
			})

			if err != nil {
				log.Printf("[%s] Got error executing command `%#v`", req.connection.addr, err)
				req.connection.conn.Write([]byte(protocol.EncodeError(err.Error())))
			}
		}
	}
}

func (s *Server) startReplicator() {
	parts := strings.Split(s.config.ReplicaOf, " ")

	if len(parts) != 2 {
		log.Fatalf("[replicator] replicaof must be of form '<HOST> <PORT>', got '%s'", s.config.ReplicaOf)
	}

	conn, err := net.Dial("tcp", strings.Join(parts, ":"))

	if err != nil {
		log.Fatalf("[replicator] Got error connecting to master %#v", err)
	}

	reader := bufio.NewReader(conn)

	// Handshake part 1
	// PING PONG

	conn.Write([]byte(protocol.EncodeArray([]string{"PING"})))
	resp, _, _ := protocol.ReadString(reader)

	if resp != "PONG" {
		log.Fatalf("[replicator] Expected PONG in response to PING, got %s", resp)
	}

	// Handshake part 2
	// REPLCONF

	conn.Write([]byte(protocol.EncodeArray([]string{
		"REPLCONF", "listening-port", s.config.Port,
	})))
	resp, _, _ = protocol.ReadString(reader)
	if resp != "OK" {
		log.Fatalf("[replicator] Expected OK in response to REPLCONF, got %s", resp)
	}

	conn.Write([]byte(protocol.EncodeArray([]string{
		"REPLCONF", "capa", "psync2",
	})))
	resp, _, _ = protocol.ReadString(reader)
	if resp != "OK" {
		log.Fatalf("[replicator] Expected OK in response to REPLCONF, got %s", resp)
	}

	// Handshake part 3
	// PSYNC

	conn.Write([]byte(protocol.EncodeArray([]string{"PSYNC", "?", "-1"})))
	resp, _, _ = protocol.ReadString(reader)

	log.Printf("[replicator] Got PSYNC response: %s", resp)

	dbfile, _ := protocol.ReadBytes(reader)

	log.Printf("Received dbfile of length %d", len(dbfile))

	db, err := rdb.LoadDatabaseFromReader(bytes.NewReader(dbfile))
	if err == nil {
		s.replicaCh <- ReplicationMessage{
			messageType: "db",
			payload:     db.Hashtable,
		}
	}

	bytesReplicated := 0

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			cmd, bytesRead, err := receiveCommand(reader)

			if err == io.EOF {
				log.Print("[replicator] Master disconnected, terminating replicator.")
				return
			} else if err != nil {
				log.Fatalf("[replicator] Got error reading from master %#v", err)
			} else {
				if cmd.Name == "REPLCONF" {
					if len(cmd.Arguments) == 2 &&
						strings.ToUpper(cmd.Arguments[0]) == "GETACK" && cmd.Arguments[1] == "*" {
						conn.Write([]byte(protocol.EncodeArray([]string{
							"REPLCONF",
							"ACK",
							strconv.Itoa(bytesReplicated),
						})))
					}
				} else {
					handler, err := cmd.Handler()

					if err != nil {
						log.Printf("[replicator] Got error getting handler for command `%#v` from master", err)
						result := protocol.EncodeError(err.Error())
						conn.Write([]byte(result))
					}

					s.commandCh <- CommandRequest{
						handler:    handler,
						connection: nil,
					}
				}

				bytesReplicated += bytesRead
			}
		}
	}
}

func (s *Server) Start() {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", s.config.Port))
	if err != nil {
		log.Fatalf("Failed to bind to port %s: %v", s.config.Port, err)
	}
	defer l.Close()

	go s.startExecutor()
	if s.replication.role == "slave" {
		go s.startReplicator()
	}

	// Accept connections in a loop, spawning goroutines for each.
	// Each connection is assigned a unique integer ID (starting at 1)
	connCounter := 1
	for {
		select {
		case <-s.ctx.Done():
			log.Println("Refusing new connections")
			return
		default:
			conn, err := l.Accept()
			if err != nil {
				log.Println("Error accepting connection: ", err.Error())
				os.Exit(1)
			}

			connectionId := connCounter
			go handleConnection(s, conn, connectionId)

			connCounter += 1
		}
	}
}

type Connection struct {
	id          int
	conn        net.Conn
	isBuffering bool
	buffer      []commands.Handler
	addr        string
	isReplica   bool
}

func receiveCommand(reader *bufio.Reader) (*commands.Command, int, error) {
	// Peek the first character to determine the datatype being sent
	c, err := reader.Peek(1)

	// TODO: Handle bytesRead properly outside of the happy path, here and in protocol.

	if err != nil {
		return nil, 0, err
	}

	var rawCommand = []string{}
	var bytesRead = 0

	switch string(c) {
	// Read standard Array commands format
	case "*":
		rawCommand, bytesRead, err = protocol.ReadArray(reader)

		if err != nil {
			return nil, 0, err
		}
	// Fallback to inline commands format
	default:
		rawLine, b, err := protocol.ReadLine(reader)
		bytesRead += b

		if err != nil {
			return nil, 0, err
		}

		rawCommand = strings.Split(rawLine, " ")
	}

	command := commands.Command{
		Name:      strings.ToUpper(rawCommand[0]),
		Arguments: rawCommand[1:],
	}

	return &command, bytesRead, nil
}

func checkReadError(err error, addr string) bool {
	if err == io.EOF {
		log.Printf("[%s] Disconnected", addr)
		return true
	} else if err != nil {
		log.Printf("[%s] Got error reading client command `%#v`", addr, err)
		return true
	}

	return false
}

func handleConnection(s *Server, conn net.Conn, connId int) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	connState := Connection{
		id:          connId,
		conn:        conn,
		isBuffering: false,
		buffer:      []commands.Handler{},
		addr:        conn.RemoteAddr().String(),
		isReplica:   false,
	}

	for {
		select {
		case <-s.ctx.Done():
			log.Printf("Closing connection %d", connState.id)
			return
		default:
			command, _, err := receiveCommand(reader)

			if checkReadError(err, connState.addr) {
				return
			}

			// Meta commands for Replication
			if command.Name == "REPLCONF" {
				// TODO: Actual implementation
				conn.Write([]byte(protocol.EncodeString("OK")))
			} else if command.Name == "PSYNC" {
				if len(command.Arguments) < 2 || command.Arguments[0] != "?" || command.Arguments[1] != "-1" {
					log.Printf("Got unexpected PSYNC arguments: %#v", command.Arguments)
					conn.Write([]byte(protocol.EncodeError("Unexpected PSYNC arguments")))
				} else {
					conn.Write([]byte(protocol.EncodeString(
						fmt.Sprintf("FULLRESYNC %s %d", s.replication.masterReplid, 0),
					)))

					dbbytes, _ := hex.DecodeString(rdb.EmptyHexDatabase)

					conn.Write([]byte(protocol.EncodeBytes(dbbytes)))

					// Mark this connection as a replica link, and add it into the global table of replicas.
					connState.isReplica = true

					s.replicaCh <- ReplicationMessage{
						messageType: "replica",
						payload:     connState,
					}
				}

				// Meta commands dealing with Transactions
			} else if command.Name == "MULTI" {
				connState.isBuffering = true
				conn.Write([]byte(protocol.EncodeString("OK")))
			} else if command.Name == "DISCARD" {
				if !connState.isBuffering {
					conn.Write([]byte(protocol.EncodeError("ERR DISCARD without MULTI")))
					return
				}

				connState.buffer = []commands.Handler{}
				connState.isBuffering = false
				conn.Write([]byte(protocol.EncodeString("OK")))
			} else if command.Name == "EXEC" {
				if !connState.isBuffering {
					conn.Write([]byte(protocol.EncodeError("ERR EXEC without MULTI")))
					return
				}

				// Join all buffered commands into a single ExecHandler, and send it to the executor
				s.commandCh <- CommandRequest{
					handler:    commands.NewExecHandler(connState.buffer),
					connection: &connState,
				}

				// Assumption: Don't need to Wait here, as grouped commands do not block.
				// Semantically this is correct, as within an EXEC no other commands can be interleaved,
				// so blocking cannot possibly receive new data.

				connState.buffer = []commands.Handler{}
				connState.isBuffering = false
			} else {
				// Otherwise just find handlers as we receive, and either buffer or send to the executor
				log.Printf("[%s] Received command: %#v", connState.addr, command)

				handler, err := command.Handler()

				if err != nil {
					log.Printf("[%s] Got error getting handler for command `%#v`", connState.addr, err)
					result := protocol.EncodeError(err.Error())
					conn.Write([]byte(result))
				}

				if connState.isBuffering {
					connState.buffer = append(connState.buffer, handler)
					conn.Write([]byte(protocol.EncodeString("QUEUED")))
				} else {
					s.commandCh <- CommandRequest{
						handler:    handler,
						connection: &connState,
					}

					// Wait for the handler to be done. For all non-blocking commands, this is a no-op.
					// For blocking commands this ensures that the commands for a single client are not
					// executed out-of-order, e.g. by a later command from the client executing before a
					// blocking command has finished or timed out.
					handler.Wait()
				}
			}
		}
	}
}
