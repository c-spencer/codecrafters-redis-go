package main

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
)

// EncodeString encodes a simple string in Redis protocol
func EncodeString(value string) string {
	return fmt.Sprintf("+%s\r\n", value)
}

// EncodeError encodes an error message in Redis protocol
func EncodeError(value string) string {
	return fmt.Sprintf("-%s\r\n", value)
}

// EncodeInteger encodes an integer in Redis protocol
func EncodeInteger(value int) string {
	return fmt.Sprintf(":%d\r\n", value)
}

// EncodeBulkString encodes a bulk string in Redis protocol
func EncodeBulkString(value string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}

// EncodeArray encodes an array in Redis protocol
func EncodeArray(values []string) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("*%d\r\n", len(values)))
	for _, value := range values {
		buffer.WriteString(EncodeBulkString(value))
	}
	return buffer.String()
}

// ReadLine reads a single line from the reader
func ReadLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	// Remove trailing \r if present
	// Conditional to support inline commands
	return strings.TrimRight(line, "\r\n"), nil
}

// Parse a BulkString or Array length line
func parseLengthLine(reader *bufio.Reader, pattern string) (int, error) {
	lengthLine, err := ReadLine(reader)
	if err != nil {
		return 0, err
	}
	var length int
	_, err = fmt.Sscanf(lengthLine, pattern, &length)
	if err != nil {
		return 0, err
	}

	return length, nil
}

// ReadBulkString reads a bulk string from the reader
func ReadBulkString(reader *bufio.Reader) (string, error) {
	length, err := parseLengthLine(reader, "$%d")
	if err != nil {
		return "", err
	}
	if length == -1 {
		return "", nil // Null bulk string
	}

	// Read the contents line, sizing the buffer to the declared length.
	buf := make([]byte, length+2)
	_, err = reader.Read(buf)
	if err != nil {
		return "", err
	}
	return string(buf[:length]), nil
}

// ReadArray reads an array from the reader
func ReadArray(reader *bufio.Reader) ([]string, error) {
	length, err := parseLengthLine(reader, "*%d")
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil // Null array
	}

	// Read number of BulkStrings as declared
	values := make([]string, length)
	for i := 0; i < length; i++ {
		value, err := ReadBulkString(reader)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}
