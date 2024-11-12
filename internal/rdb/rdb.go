package rdb

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type ValueEntry struct {
	Key    string
	Value  string
	Expiry *time.Time
}

func readHeader(reader *bufio.Reader) int {
	buffer := make([]byte, 9)

	_, err := io.ReadFull(reader, buffer)

	if err != nil {
		panic("Got error reading file header")
	}

	headerString := string(buffer)
	if !strings.HasPrefix(headerString, "REDIS") {
		panic("File header does not contain expected REDIS")
	}

	versionNumber, err := strconv.Atoi(headerString[5:])
	if err != nil {
		panic("Got error decoding file version number.")
	}

	return versionNumber
}

func readString(reader *bufio.Reader) string {
	strLen, isString := readSize(reader)

	if isString {
		return strconv.Itoa(strLen)
	} else {
		strBuffer := make([]byte, strLen)
		io.ReadFull(reader, strBuffer)
		return string(strBuffer)
	}
}

func readMetadata(reader *bufio.Reader) map[string]string {
	peeked, err := reader.Peek(1)
	if err != nil {
		panic("Couldn't peek metadata")
	}
	if peeked[0] != 0xFA {
		panic("Expected 0xFA marker byte as metadata header section")
	}
	reader.Discard(1)

	metadata := map[string]string{}

	for peeked[0] != 0xFE {
		peeked, err = reader.Peek(1)
		if err != nil {
			panic("Couldn't peek reader")
		}
		if peeked[0] == 0xFE {
			break
		} else if peeked[0] == 0xFA {
			reader.Discard(1)
		}

		name := readString(reader)
		value := readString(reader)

		metadata[name] = value
	}

	return metadata
}

// Read a size value. Boolean flag indicates whether to treat the return as the full string.
func readSize(reader *bufio.Reader) (int, bool) {
	headerByte, _ := reader.ReadByte()

	switch (headerByte & 0b11000000) >> 6 {
	case 0:
		// Lower 6 bits are the number encoded
		return int(headerByte), false
	case 1:
		// Lower 6 bits combined with the next 8 bits (big-endian) encode a 14bit number
		nextByte, _ := reader.ReadByte()
		return int(headerByte&0b00111111)<<8 | int(nextByte), false
	case 2:
		// Header byte is discarded, just read next 4 bytes as a big-endian value.
		intval := uint32(0)
		binary.Read(reader, binary.BigEndian, &intval)
		return int(intval), false
	case 3:
		// String endoded values
		switch headerByte {
		case 0xC0:
			v := uint8(0)
			binary.Read(reader, binary.LittleEndian, &v)
			return int(v), true
		case 0xC1:
			v := uint16(0)
			binary.Read(reader, binary.LittleEndian, &v)
			return int(v), true
		case 0xC2:
			v := uint32(0)
			binary.Read(reader, binary.LittleEndian, &v)
			return int(v), true
		case 0xC3:
			panic("TODO: LZF string encoded values")
		default:
			panic("Unknown size encoding")
		}
	default:
		panic("readSize unreachable branch")
	}
}

func readDatabase(reader *bufio.Reader) map[string]*ValueEntry {
	b, err := reader.ReadByte()
	if err != nil || b != 0xFE {
		panic("Expected 0xFE marker byte as database section marker")
	}

	readSize(reader) // Skip databaseIndex

	b, err = reader.ReadByte()
	if err != nil || b != 0xFB {
		panic("Expected 0xFB marker byte as hashtable marker")
	}

	tableSize, _ := readSize(reader)
	readSize(reader) // Skip expiringKeySize

	hashtable := make(map[string]*ValueEntry, tableSize)

	for {
		b, _ = reader.ReadByte()

		switch b {

		case 0xFF:
			// End of file section marker
			return hashtable

		case 0xFC:
			// Millisecond expiring key
			timestamp := uint64(0)
			binary.Read(reader, binary.LittleEndian, &timestamp)
			expiry := time.UnixMilli(int64(timestamp))

			// TODO: Handle other types
			b, _ = reader.ReadByte()
			if b != 0x00 {
				panic("Expected string to follow expiration")
			}

			name := readString(reader)
			value := readString(reader)

			// Skip loading expired values
			if expiry.Before(time.Now()) {
				continue
			}

			hashtable[name] = &ValueEntry{
				Key:    name,
				Value:  value,
				Expiry: &expiry,
			}

		case 0xFD:
			// Second expiring key
			timestamp := uint32(0)
			binary.Read(reader, binary.LittleEndian, &timestamp)
			expiry := time.Unix(int64(timestamp), 0)

			// TODO: Handle other types
			b, _ = reader.ReadByte()
			if b != 0x00 {
				panic("Expected string to follow expiration")
			}

			name := readString(reader)
			value := readString(reader)

			// Skip loading expired values
			if expiry.Before(time.Now()) {
				continue
			}

			hashtable[name] = &ValueEntry{
				Key:    name,
				Value:  value,
				Expiry: &expiry,
			}

		case 0x00:
			// String entry
			name := readString(reader)
			value := readString(reader)

			hashtable[name] = &ValueEntry{
				Key:    name,
				Value:  value,
				Expiry: nil,
			}
		}
	}
}

type LoadedDatabase struct {
	Version   int
	Metadata  map[string]string
	Hashtable map[string]*ValueEntry
}

func LoadDatabase(path string) (*LoadedDatabase, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	version := readHeader(reader)
	metadata := readMetadata(reader)
	hashtable := readDatabase(reader)

	return &LoadedDatabase{
		Version:   version,
		Metadata:  metadata,
		Hashtable: hashtable,
	}, nil
}
