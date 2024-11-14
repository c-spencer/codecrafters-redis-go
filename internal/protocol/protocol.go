package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
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

func EncodeNullBulkString() string {
	return "$-1\r\n"
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

// EncodeEncodedArray encodes an array of already encoded strings
func EncodeEncodedArray(values []string) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("*%d\r\n", len(values)))
	for _, value := range values {
		buffer.WriteString(value)
	}
	return buffer.String()
}

// EncodeBytes encodes a byte slice in Redis protocol
func EncodeBytes(value []byte) string {
	return fmt.Sprintf("$%d\r\n%s", len(value), value)
}

// ReadLine reads a single line from the reader
func ReadLine(reader *bufio.Reader) (string, int, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", 0, err
	}
	// Remove trailing \r if present
	// Conditional to support inline commands
	return strings.TrimRight(line, "\r\n"), len(line), nil
}

// Parse a BulkString or Array length line
func parseLengthLine(reader *bufio.Reader, pattern string) (int, int, error) {
	lengthLine, bytesRead, err := ReadLine(reader)
	if err != nil {
		return 0, 0, err
	}
	var length int
	_, err = fmt.Sscanf(lengthLine, pattern, &length)
	if err != nil {
		return 0, 0, err
	}

	return length, bytesRead, nil
}

// ReadBulkString reads a bulk string from the reader
func ReadBulkString(reader *bufio.Reader) (string, int, error) {
	totalBytesRead := 0

	length, bytesRead, err := parseLengthLine(reader, "$%d")
	totalBytesRead += bytesRead

	if err != nil {
		return "", totalBytesRead, err
	}
	if length == -1 {
		return "", totalBytesRead, nil // Null bulk string
	}

	// Read the contents line, sizing the buffer to the declared length.
	buf := make([]byte, length+2)
	bytesRead, err = io.ReadFull(reader, buf)
	totalBytesRead += bytesRead

	if err != nil {
		return "", totalBytesRead, err
	}
	return string(buf[:length]), totalBytesRead, nil
}

func ReadBytes(reader *bufio.Reader) ([]byte, error) {
	length, _, err := parseLengthLine(reader, "$%d")
	if err != nil {
		return []byte{}, err
	}
	if length == -1 {
		return []byte{}, nil // Null binary
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return []byte{}, err
	}

	return buf, nil
}

// ReadString reads a simple string from the reader
func ReadString(reader *bufio.Reader) (string, int, error) {
	line, bytesRead, err := ReadLine(reader)
	if err != nil {
		return "", 0, err
	}
	if len(line) < 1 || line[0] != '+' {
		return "", bytesRead, fmt.Errorf("invalid simple string format")
	}
	return line[1:], bytesRead, nil
}

// ReadInteger reads an integer from the reader
func ReadInteger(reader *bufio.Reader) (int, int, error) {
	line, bytesRead, err := ReadLine(reader)
	if err != nil {
		return 0, 0, err
	}
	if len(line) < 1 || line[0] != ':' {
		return 0, bytesRead, fmt.Errorf("invalid integer format")
	}
	var value int
	_, err = fmt.Sscanf(line[1:], "%d", &value)
	if err != nil {
		return 0, bytesRead, err
	}
	return value, bytesRead, nil
}

// ReadArray reads an array from the reader
func ReadArray(reader *bufio.Reader) ([]string, int, error) {
	totalBytesRead := 0

	length, bytesRead, err := parseLengthLine(reader, "*%d")
	totalBytesRead += bytesRead

	if err != nil {
		return nil, totalBytesRead, err
	}
	if length == -1 {
		return nil, totalBytesRead, nil // Null array
	}

	// Read number of BulkStrings as declared
	values := make([]string, length)
	for i := 0; i < length; i++ {
		value, bytesRead, err := ReadBulkString(reader)
		totalBytesRead += bytesRead

		if err != nil {
			return nil, totalBytesRead, err
		}
		values[i] = value
	}
	return values, totalBytesRead, nil
}
