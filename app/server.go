package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

type ServerState struct {
	mutex  sync.RWMutex
	values map[string]string
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here.")

	state := ServerState{
		mutex:  sync.RWMutex{},
		values: map[string]string{},
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

			// Format as Bulk String and return response.
			conn.Write([]byte(EncodeBulkString(rawCommand[1])))

		case "SET":
			if len(rawCommand) != 3 {
				log.Printf("[%s] Malformed SET request: %#v", addr, rawCommand)
				conn.Write([]byte(EncodeError("Malformed request.")))
				continue
			}

			state.mutex.Lock()
			state.values[rawCommand[1]] = rawCommand[2]
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

			if exists {
				conn.Write([]byte(EncodeBulkString(value)))
			} else {
				conn.Write([]byte(EncodeBulkString("nil")))
			}

		default:
			log.Printf("[%s] Unknown command '%s'", addr, command)
			conn.Write([]byte(EncodeError("Unknown Command")))
		}
	}
}
