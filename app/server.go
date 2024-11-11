package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
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

	buf := make([]byte, 1024)
	addr := conn.RemoteAddr().String()

	for {
		length, err := conn.Read(buf)

		if err == io.EOF {
			log.Printf("[%s] Connection Closed\n", addr)
			return
		} else if err != nil {
			log.Printf("Error reading: %#v\n", err)
			return
		}

		rawMessage := string(buf[:length])
		lines := strings.Split(rawMessage, "\r\n")

		if len(lines) == 0 {
			continue
		}

		// For now, assume command is sent as a Bulk String

		elements := []string{}

		// Assume the first line is correct, and parse out the rest.

		for i := 1; i < len(lines); i++ {
			if strings.HasPrefix(lines[i], "$") {
				elementLength, err := strconv.Atoi(lines[i][1:])
				if err != nil {
					log.Printf("[%s] Error parsing element length: %#v\n", addr, err)
				}

				if i+1 < len(lines) && len(lines[i+1]) == elementLength {
					elements = append(elements, lines[i+1])
					i++
				} else {
					log.Printf("[%s] Invalid Bulk String command received: %#v", addr, lines)
					return
				}
			}
		}

		command := strings.ToUpper(elements[0])
		log.Printf("[%s] Received command %s", addr, command)

		switch command {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(elements) != 2 {
				log.Printf("[%s] Malformed ECHO request: %#v", addr, elements)
				return
			}

			// Format as Bulk String and return response.
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(elements[1]), elements[1])

			conn.Write([]byte(resp))

		default:
			log.Printf("[%s] Unknown command '%s'", addr, elements[0])
		}
	}
}
