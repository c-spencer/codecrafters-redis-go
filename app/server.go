package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
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

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	addr := conn.RemoteAddr().String()

	for {
		// For now, assume command is sent as a Bulk String

		rawCommand, err := ReadArray(reader)
		if err == io.EOF {
			log.Printf("[%s] Disconnected", addr)
			return
		}
		if err != nil {
			log.Printf("[%s] Got error reading client command `%#v`", addr, err)
			return
		}

		command := strings.ToUpper(rawCommand[0])
		log.Printf("[%s] Received command %s", addr, command)

		switch command {
		case "PING":
			conn.Write([]byte(EncodeString("PONG")))
		case "ECHO":
			if len(rawCommand) != 2 {
				log.Printf("[%s] Malformed ECHO request: %#v", addr, rawCommand)
				return
			}

			// Format as Bulk String and return response.
			conn.Write([]byte(EncodeBulkString(rawCommand[1])))

		default:
			log.Printf("[%s] Unknown command '%s'", addr, command)
			conn.Write([]byte(EncodeError("Unknown Command")))
		}
	}
}
