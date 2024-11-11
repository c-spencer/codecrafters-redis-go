package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ServerState struct {
	mutex  sync.RWMutex
	values map[string]ValueEntry
	config map[string]string
}

type ValueEntry struct {
	key    string
	value  string
	expiry *time.Time
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

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here.")

	state := ServerState{
		mutex:  sync.RWMutex{},
		values: map[string]ValueEntry{},
		config: map[string]string{
			"dir":        dir,
			"dbfilename": dbfilename,
		},
	}

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

func handleConnection(conn net.Conn, state *ServerState) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	addr := conn.RemoteAddr().String()

	for {
		// For now, assume command is sent as a Bulk String

		// Peek the first character to determine the datatype being sent
		c, err := reader.Peek(1)

		if checkReadError(err, addr) {
			return
		}

		var rawCommand = []string{}

		switch string(c) {
		// Read standard Array commands format
		case "*":
			rawCommand, err = ReadArray(reader)

			if checkReadError(err, addr) {
				return
			}
		// Fallback to inline commands format
		default:
			rawLine, err := ReadLine(reader)

			if checkReadError(err, addr) {
				return
			}

			rawCommand = strings.Split(rawLine, " ")
		}

		command := strings.ToUpper(rawCommand[0])
		log.Printf("[%s] Received command %s", addr, command)

		switch command {
		case "PING":
			conn.Write([]byte(EncodeString("PONG")))

		case "ECHO":
			if len(rawCommand) != 2 {
				log.Printf("[%s] Malformed ECHO request: %#v", addr, rawCommand)
				conn.Write([]byte(EncodeError("Malformed request.")))
				continue
			}

			conn.Write([]byte(EncodeBulkString(rawCommand[1])))

		case "SET":
			if len(rawCommand) < 3 {
				log.Printf("[%s] Malformed SET request: %#v", addr, rawCommand)
				conn.Write([]byte(EncodeError("Malformed request.")))
				continue
			}

			value := ValueEntry{
				key:    rawCommand[1],
				value:  rawCommand[2],
				expiry: nil,
			}

			if len(rawCommand) >= 5 {
				// TODO: Proper handling of arguments checking
				expiry, err := strconv.Atoi(rawCommand[4])

				if err != nil {
					log.Printf("[%s] Malformed SET request: %#v", addr, rawCommand)
					conn.Write([]byte(EncodeError("Malformed request.")))
					continue
				}

				if strings.ToUpper(rawCommand[3]) == "PX" {
					t := time.Now().Add(time.Duration(expiry) * time.Millisecond)
					value.expiry = &t
				} else if strings.ToUpper(rawCommand[3]) == "EX" {
					t := time.Now().Add(time.Duration(expiry) * time.Second)
					value.expiry = &t
				}

				// TODO: Send to the Reaper
			}

			state.mutex.Lock()
			state.values[value.key] = value
			state.mutex.Unlock()

			conn.Write([]byte(EncodeString("OK")))

		case "GET":
			if len(rawCommand) != 2 {
				log.Printf("[%s] Malformed GET request: %#v", addr, rawCommand)
				conn.Write([]byte(EncodeError("Malformed request.")))
				continue
			}

			state.mutex.RLock()
			value, exists := state.values[rawCommand[1]]
			state.mutex.RUnlock()

			if exists && (value.expiry == nil || value.expiry.After(time.Now())) {
				conn.Write([]byte(EncodeBulkString(value.value)))
			} else {
				conn.Write([]byte(EncodeNullBulkString()))
			}
			// TODO: Send to the Reaper

		case "CONFIG":
			if len(rawCommand) == 3 && strings.ToUpper(rawCommand[1]) == "GET" {
				value, exists := state.config[rawCommand[2]]

				if exists {
					conn.Write([]byte(EncodeArray([]string{rawCommand[2], value})))
				} else {
					conn.Write([]byte(EncodeNullBulkString()))
				}
			} else {
				log.Printf("[%s] Malformed GET request: %#v", addr, rawCommand)
				conn.Write([]byte(EncodeError("Malformed request.")))
				continue
			}

		default:
			log.Printf("[%s] Unknown command '%s'", addr, command)
			conn.Write([]byte(EncodeError("Unknown Command")))
		}
	}
}
