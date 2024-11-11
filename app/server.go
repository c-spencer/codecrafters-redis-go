package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here.")

	// Bind to port
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	defer conn.Close()

	buf := make([]byte, 128)

	_, err = conn.Read(buf)
	if err != nil {
		panic("Could not read command.")
	}

	log.Printf("[%s][command] %s", conn.RemoteAddr().String(), buf)

	_, err = conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		panic("Could not write response.")
	}

	_, err = conn.Read(buf)
	if err != nil {
		panic("Could not read command.")
	}

	log.Printf("[%s][command] %s", conn.RemoteAddr().String(), buf)

	_, err = conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		panic("Could not write response.")
	}
}
