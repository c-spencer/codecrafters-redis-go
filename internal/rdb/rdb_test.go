package rdb

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"testing"
)

func TestReadString(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "redis-ver",
			input:    []byte{0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72},
			expected: "redis-ver",
		},
		{
			name:     "6.0.16",
			input:    []byte{0x06, 0x36, 0x2E, 0x30, 0x2E, 0x31, 0x36},
			expected: "6.0.16",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader(tt.input))
			result, err := readString(reader)
			if err != nil {
				t.Error(err)
			} else if result != tt.expected {
				t.Errorf("readSize() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestReadSize(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected int
		isString bool
	}{
		{
			name:     "6-bit integer",
			input:    []byte{0x0A}, // 18 in decimal
			expected: 10,
			isString: false,
		},
		{
			name:     "14-bit integer",
			input:    []byte{0x42, 0xBC}, // 255 in decimal
			expected: 700,
			isString: false,
		},
		{
			name:     "32-bit integer",
			input:    []byte{0x80, 0x00, 0x00, 0x42, 0x68}, // 256 in decimal
			expected: 17000,
			isString: false,
		},
		{
			name:     "8-bit encoded integer",
			input:    []byte{0xC0, 0x7B}, // 255 in decimal
			expected: 123,
			isString: true,
		},
		{
			name:     "16-bit encoded integer",
			input:    []byte{0xC1, 0x39, 0x30}, // 255 in decimal
			expected: 12345,
			isString: true,
		},
		{
			name:     "32-bit encoded integer",
			input:    []byte{0xC2, 0x87, 0xD6, 0x12, 0x00}, // 255 in decimal
			expected: 1234567,
			isString: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader(tt.input))
			result, isString, err := readSize(reader)
			if err != nil {
				t.Error(err)
			} else if result != tt.expected || isString != tt.isString {
				t.Errorf("readSize() = %v (%t), want %v (%t)", result, isString, tt.expected, tt.isString)
			}
		})
	}
}

func FuzzLoadDatabaseFromReader(f *testing.F) {
	// Give an empty database as a seed
	emptyDb, err := hex.DecodeString(EmptyHexDatabase)
	if err != nil {
		f.Fatalf("Failed to decode empty database: %v", err)
	}
	f.Add(emptyDb)

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := bufio.NewReader(bytes.NewReader(data))
		_, err := LoadDatabaseFromReader(reader)
		if err != nil {
			t.Logf("Got error: %v", err)
		}
	})
}
