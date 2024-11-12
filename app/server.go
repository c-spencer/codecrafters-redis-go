package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/protocol"
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
)

type ServerState struct {
	mutex  sync.RWMutex
	values map[string]*rdb.ValueEntry
	config map[string]string
}

func main() {
	dir := "/tmp"
	dbfilename := "dump.rdb"

	for i, arg := range os.Args {
		if arg == "--dir" && i+1 < len(os.Args) {
			dir = os.Args[i+1]
		} else if arg == "--dbfilename" && i+1 < len(os.Args) {
			dbfilename = os.Args[i+1]
		}
	}

	config := map[string]string{
		"dir":        dir,
		"dbfilename": dbfilename,
	}

	state := ServerState{
		mutex:  sync.RWMutex{},
		values: map[string]*rdb.ValueEntry{},
		config: config,
	}

	db, err := rdb.LoadDatabase(path.Join(dir, dbfilename))

	// If database loaded without error, use its state.
	if err == nil {
		state.values = db.Hashtable
	}

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here.")

	// Bind to port
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn, &state)
	}
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

func respondToBadCommand(conn *ConnState, command *Command) {
	log.Printf("[%s] Malformed %s request: %#v", conn.addr, command.name, command)
	conn.conn.Write([]byte(protocol.EncodeError("Malformed request.")))
}

type Command struct {
	name      string
	arguments []string
}

func receiveCommand(reader *bufio.Reader) (*Command, error) {
	// Peek the first character to determine the datatype being sent
	c, err := reader.Peek(1)

	if err != nil {
		return nil, err
	}

	var rawCommand = []string{}

	switch string(c) {
	// Read standard Array commands format
	case "*":
		rawCommand, err = protocol.ReadArray(reader)

		if err != nil {
			return nil, err
		}
	// Fallback to inline commands format
	default:
		rawLine, err := protocol.ReadLine(reader)

		if err != nil {
			return nil, err
		}

		rawCommand = strings.Split(rawLine, " ")
	}

	command := Command{
		name:      strings.ToUpper(rawCommand[0]),
		arguments: rawCommand[1:],
	}

	return &command, nil
}

type ConnState struct {
	conn        net.Conn
	isBuffering bool
	buffer      []*Command
	addr        string
}

func processCommand(conn *ConnState, command *Command, state *ServerState) {
	log.Printf("[%s] Received command %s", conn.addr, command.name)

	switch command.name {
	case "PING":
		conn.conn.Write([]byte(protocol.EncodeString("PONG")))

	case "ECHO":
		if len(command.arguments) != 1 {
			respondToBadCommand(conn, command)
			return
		}

		conn.conn.Write([]byte(protocol.EncodeBulkString(command.arguments[0])))

	case "SET":
		if len(command.arguments) < 2 {
			respondToBadCommand(conn, command)
			return
		}

		var expiry *time.Time = nil

		if len(command.arguments) >= 4 {
			// TODO: Proper handling of arguments checking
			duration, err := strconv.Atoi(command.arguments[3])

			if err != nil {
				respondToBadCommand(conn, command)
				return
			}

			if strings.ToUpper(command.arguments[2]) == "PX" {
				t := time.Now().Add(time.Duration(duration) * time.Millisecond)
				expiry = &t
			} else if strings.ToUpper(command.arguments[2]) == "EX" {
				t := time.Now().Add(time.Duration(duration) * time.Second)
				expiry = &t
			}

			// TODO: Send to the Reaper
		}

		value := rdb.ValueEntry{
			Key:    command.arguments[0],
			Value:  command.arguments[1],
			Expiry: expiry,
		}

		state.mutex.Lock()
		state.values[value.Key] = &value
		state.mutex.Unlock()

		conn.conn.Write([]byte(protocol.EncodeString("OK")))

	case "GET":
		if len(command.arguments) != 1 {
			respondToBadCommand(conn, command)
			return
		}

		state.mutex.RLock()
		value, exists := state.values[command.arguments[0]]
		state.mutex.RUnlock()

		if exists && (value.Expiry == nil || value.Expiry.After(time.Now())) {
			conn.conn.Write([]byte(protocol.EncodeBulkString(value.Value)))
		} else {
			conn.conn.Write([]byte(protocol.EncodeNullBulkString()))
		}
		// TODO: Send to the Reaper

	case "KEYS":
		state.mutex.RLock()
		keys := make([]string, len(state.values))

		i := 0
		for k := range state.values {
			keys[i] = k
			i++
		}

		state.mutex.RUnlock()

		conn.conn.Write([]byte(protocol.EncodeArray(keys)))

	case "INCR":
		if len(command.arguments) < 1 {
			respondToBadCommand(conn, command)
			return
		}

		var x = 0
		var err error = nil

		state.mutex.Lock()

		current, exists := state.values[command.arguments[0]]

		// If exists and unexpired, increment by 1
		if exists && (current.Expiry == nil || current.Expiry.After(time.Now())) {
			x, err = strconv.Atoi(current.Value)

			if err != nil {
				state.mutex.Unlock()
				conn.conn.Write([]byte(protocol.EncodeError("ERR value is not an integer or out of range")))
				return
			}

			x += 1
			current.Value = strconv.Itoa(x)
		} else {
			// Otherwise create the key and give it the value 1
			x = 1

			value := rdb.ValueEntry{
				Key:    command.arguments[0],
				Value:  "1",
				Expiry: nil,
			}

			state.values[command.arguments[0]] = &value
		}

		state.mutex.Unlock()

		conn.conn.Write([]byte(protocol.EncodeInteger(x)))

	case "CONFIG":
		if len(command.arguments) == 2 && strings.ToUpper(command.arguments[0]) == "GET" {
			value, exists := state.config[command.arguments[1]]

			if exists {
				conn.conn.Write([]byte(protocol.EncodeArray([]string{command.arguments[1], value})))
			} else {
				conn.conn.Write([]byte(protocol.EncodeNullBulkString()))
			}
		} else {
			respondToBadCommand(conn, command)
		}

	case "MULTI":
		conn.isBuffering = true
		conn.conn.Write([]byte(protocol.EncodeString("OK")))

	default:
		respondToBadCommand(conn, command)
	}
}

func handleConnection(conn net.Conn, state *ServerState) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	connState := ConnState{
		conn:        conn,
		isBuffering: false,
		buffer:      []*Command{},
		addr:        conn.RemoteAddr().String(),
	}

	for {
		// Peek the first character to determine the datatype being sent
		command, err := receiveCommand(reader)

		if checkReadError(err, connState.addr) {
			return
		}

		if command.name == "MULTI" {
			connState.isBuffering = true
			connState.conn.Write([]byte(protocol.EncodeString("OK")))
		} else if command.name == "EXEC" {
			if !connState.isBuffering {
				connState.conn.Write([]byte(protocol.EncodeError("ERR EXEC without MULTI")))
				return
			}
			// TODO: Execute commands

			connState.conn.Write([]byte(protocol.EncodeArray([]string{})))
			connState.buffer = []*Command{}
			connState.isBuffering = false
		} else if connState.isBuffering {
			connState.buffer = append(connState.buffer, command)
			connState.conn.Write([]byte(protocol.EncodeString("QUEUED")))
		} else {
			processCommand(&connState, command, state)
		}
	}
}
