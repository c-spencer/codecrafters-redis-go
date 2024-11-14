package protocol

import (
	"bufio"
	"strings"
	"testing"
)

func TestParseLengthLine(t *testing.T) {
	tests := []struct {
		input         string
		pattern       string
		expectedLen   int
		expectedBytes int
		err           bool
	}{
		{input: "$6\r\n", pattern: "$%d", expectedLen: 6, expectedBytes: 4, err: false},
		{input: "*2\r\n", pattern: "*%d", expectedLen: 2, expectedBytes: 4, err: false},
		{input: "$-1\r\n", pattern: "$%d", expectedLen: -1, expectedBytes: 5, err: false},
		{input: "*-1\r\n", pattern: "*%d", expectedLen: -1, expectedBytes: 5, err: false},
		{input: "$abc\r\n", pattern: "$%d", expectedLen: 0, expectedBytes: 0, err: true},
	}

	for _, test := range tests {
		reader := bufio.NewReader(strings.NewReader(test.input))
		length, bytesRead, err := parseLengthLine(reader, test.pattern)
		if (err != nil) != test.err {
			t.Errorf("parseLengthLine(%q, %q) error = %v, wantErr %v", test.input, test.pattern, err, test.err)
			continue
		}
		if length != test.expectedLen {
			t.Errorf("parseLengthLine(%q, %q) length = %v, want %v", test.input, test.pattern, length, test.expectedLen)
		}
		if bytesRead != test.expectedBytes {
			t.Errorf("parseLengthLine(%q, %q) bytesRead = %v, want %v", test.input, test.pattern, bytesRead, test.expectedBytes)
		}
	}
}

func TestReadBulkString(t *testing.T) {
	tests := []struct {
		input         string
		expected      string
		expectedBytes int
		err           bool
	}{
		{input: "$6\r\nfoobar\r\n", expected: "foobar", expectedBytes: 12, err: false},
		{input: "$0\r\n\r\n", expected: "", expectedBytes: 6, err: false},
		{input: "$-1\r\n", expected: "", expectedBytes: 5, err: false},
		{input: "$3\r\nfoo\r\n", expected: "foo", expectedBytes: 9, err: false},
		{input: "$3\r\nfo", expected: "", expectedBytes: 6, err: true},
	}

	for _, test := range tests {
		reader := bufio.NewReader(strings.NewReader(test.input))
		result, bytesRead, err := ReadBulkString(reader)
		if (err != nil) != test.err {
			t.Errorf("ReadBulkString(%q) error = %v, wantErr %v", test.input, err, test.err)
			continue
		}
		if result != test.expected {
			t.Errorf("ReadBulkString(%q) = %v, want %v", test.input, result, test.expected)
		}
		if bytesRead != test.expectedBytes {
			t.Errorf("ReadBulkString(%q) bytesRead = %v, want %v", test.input, bytesRead, test.expectedBytes)
		}
	}
}

func TestReadBytes(t *testing.T) {
	tests := []struct {
		input    string
		expected []byte
		err      bool
	}{
		{input: "$6\r\nfoobar", expected: []byte("foobar"), err: false},
		{input: "$0\r\n", expected: []byte(""), err: false},
		{input: "$-1\r\n", expected: nil, err: false},
		{input: "$3\r\nfoo", expected: []byte("foo"), err: false},
		{input: "$3\r\nfo", expected: nil, err: true},
	}

	for _, test := range tests {
		reader := bufio.NewReader(strings.NewReader(test.input))
		result, err := ReadBytes(reader)
		if (err != nil) != test.err {
			t.Errorf("ReadBytes(%q) error = %v, wantErr %v", test.input, err, test.err)
			continue
		}
		if string(result) != string(test.expected) {
			t.Errorf("ReadBytes(%q) = %v, want %v", test.input, result, test.expected)
		}
	}
}

func TestReadString(t *testing.T) {
	tests := []struct {
		input         string
		expected      string
		expectedBytes int
		err           bool
	}{
		{input: "+OK\r\n", expected: "OK", expectedBytes: 5, err: false},
		{input: "+PONG\r\n", expected: "PONG", expectedBytes: 7, err: false},
		{input: "+\r\n", expected: "", expectedBytes: 3, err: false},
		{input: "-Error message\r\n", expected: "", expectedBytes: 16, err: true},
		{input: "Invalid\r\n", expected: "", expectedBytes: 9, err: true},
	}

	for _, test := range tests {
		reader := bufio.NewReader(strings.NewReader(test.input))
		result, bytesRead, err := ReadString(reader)
		if (err != nil) != test.err {
			t.Errorf("ReadString(%q) error = %v, wantErr %v", test.input, err, test.err)
			continue
		}
		if result != test.expected {
			t.Errorf("ReadString(%q) = %v, want %v", test.input, result, test.expected)
		}
		if bytesRead != test.expectedBytes {
			t.Errorf("ReadString(%q) bytesRead = %v, want %v", test.input, bytesRead, test.expectedBytes)
		}
	}
}

func TestReadArray(t *testing.T) {
	tests := []struct {
		input         string
		expected      []string
		expectedBytes int
		err           bool
	}{
		{input: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", expected: []string{"foo", "bar"}, expectedBytes: 22, err: false},
		{input: "*0\r\n", expected: []string{}, expectedBytes: 4, err: false},
		{input: "*-1\r\n", expected: nil, expectedBytes: 5, err: false},
		{input: "*1\r\n$3\r\nfoo\r\n", expected: []string{"foo"}, expectedBytes: 13, err: false},
		{input: "*2\r\n$3\r\nfoo\r\n$3\r\nba", expected: nil, expectedBytes: 19, err: true},
	}

	for _, test := range tests {
		reader := bufio.NewReader(strings.NewReader(test.input))
		result, bytesRead, err := ReadArray(reader)
		if (err != nil) != test.err {
			t.Errorf("ReadArray(%q) error = %v, wantErr %v", test.input, err, test.err)
			continue
		}
		if len(result) != len(test.expected) {
			t.Errorf("ReadArray(%q) length = %v, want %v", test.input, len(result), len(test.expected))
		}
		for i := range result {
			if result[i] != test.expected[i] {
				t.Errorf("ReadArray(%q) = %v, want %v", test.input, result, test.expected)
			}
		}
		if bytesRead != test.expectedBytes {
			t.Errorf("ReadArray(%q) bytesRead = %v, want %v", test.input, bytesRead, test.expectedBytes)
		}
	}
}
