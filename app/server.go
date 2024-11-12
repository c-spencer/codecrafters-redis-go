package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/protocol"
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
)

type ValueWatchChannel chan *rdb.ValueEntry

type ServerState struct {
	mutex    sync.RWMutex
	values   map[string]*rdb.ValueEntry
	config   map[string]string
	watchers map[int]*ValueWatchChannel
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

	config := map[string]string{
		"dir":        dir,
		"dbfilename": dbfilename,
	}

	state := ServerState{
		mutex:    sync.RWMutex{},
		values:   map[string]*rdb.ValueEntry{},
		config:   config,
		watchers: map[int]*ValueWatchChannel{},
	}

	db, err := rdb.LoadDatabase(path.Join(dir, dbfilename))

	// If database loaded without error, use its state.
	if err == nil {
		state.values = db.Hashtable
	}

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here.")

	// Bind to port
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	connCounter := 1

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn, connCounter, &state)

		connCounter += 1
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

func respondToBadCommand(conn *ConnState, command *Command) {
	log.Printf("[%s] Malformed %s request: %#v", conn.addr, command.name, command)
	conn.conn.Write([]byte(protocol.EncodeError("Malformed request.")))
}

type Command struct {
	name      string
	arguments []string
}

func receiveCommand(reader *bufio.Reader) (*Command, error) {
	// Peek the first character to determine the datatype being sent
	c, err := reader.Peek(1)

	if err != nil {
		return nil, err
	}

	var rawCommand = []string{}

	switch string(c) {
	// Read standard Array commands format
	case "*":
		rawCommand, err = protocol.ReadArray(reader)

		if err != nil {
			return nil, err
		}
	// Fallback to inline commands format
	default:
		rawLine, err := protocol.ReadLine(reader)

		if err != nil {
			return nil, err
		}

		rawCommand = strings.Split(rawLine, " ")
	}

	command := Command{
		name:      strings.ToUpper(rawCommand[0]),
		arguments: rawCommand[1:],
	}

	return &command, nil
}

type ConnState struct {
	id          int
	conn        net.Conn
	isBuffering bool
	buffer      []*Command
	addr        string
}

func encodeEntry(entry *rdb.StreamEntry) string {
	props := []string{}

	for j := range entry.Properties {
		props = append(props, entry.Properties[j].Key, entry.Properties[j].Value)
	}

	return protocol.EncodeEncodedArray([]string{
		protocol.EncodeString(entry.Id.String()),
		protocol.EncodeArray(props),
	})
}

func processCommand(conn *ConnState, command *Command, state *ServerState) *string {
	log.Printf("[%s] Received command %s", conn.addr, command.name)

	switch command.name {
	case "PING":
		result := protocol.EncodeString("PONG")
		return &result

	case "ECHO":
		if len(command.arguments) != 1 {
			respondToBadCommand(conn, command)
			return nil
		}

		result := protocol.EncodeBulkString(command.arguments[0])
		return &result

	case "SET":
		if len(command.arguments) < 2 {
			respondToBadCommand(conn, command)
			return nil
		}

		var expiry *time.Time = nil

		if len(command.arguments) >= 4 {
			// TODO: Proper handling of arguments checking
			duration, err := strconv.Atoi(command.arguments[3])

			if err != nil {
				respondToBadCommand(conn, command)
				return nil
			}

			if strings.ToUpper(command.arguments[2]) == "PX" {
				t := time.Now().Add(time.Duration(duration) * time.Millisecond)
				expiry = &t
			} else if strings.ToUpper(command.arguments[2]) == "EX" {
				t := time.Now().Add(time.Duration(duration) * time.Second)
				expiry = &t
			}

			// TODO: Send to the Reaper
		}

		value := rdb.ValueEntry{
			Key:    command.arguments[0],
			Value:  command.arguments[1],
			Type:   rdb.TString,
			Expiry: expiry,
		}

		state.mutex.Lock()
		state.values[value.Key] = &value
		state.mutex.Unlock()

		result := protocol.EncodeString("OK")
		return &result

	case "GET":
		if len(command.arguments) != 1 {
			respondToBadCommand(conn, command)
			return nil
		}

		state.mutex.RLock()
		value, exists := state.values[command.arguments[0]]
		state.mutex.RUnlock()

		if exists && (value.Expiry == nil || value.Expiry.After(time.Now())) {
			// TODO: Check type
			result := protocol.EncodeBulkString(value.Value.(string))
			return &result
		} else {
			result := protocol.EncodeNullBulkString()
			return &result
		}
		// TODO: Send to the Reaper

	case "KEYS":
		state.mutex.RLock()
		keys := make([]string, len(state.values))

		i := 0
		for k := range state.values {
			keys[i] = k
			i++
		}

		state.mutex.RUnlock()

		result := protocol.EncodeArray(keys)
		return &result

	case "INCR":
		if len(command.arguments) < 1 {
			respondToBadCommand(conn, command)
			return nil
		}

		var x = 0
		var err error = nil

		state.mutex.Lock()

		current, exists := state.values[command.arguments[0]]

		// If exists and unexpired, increment by 1
		if exists && (current.Expiry == nil || current.Expiry.After(time.Now())) {
			// TODO: Check type is string
			x, err = strconv.Atoi(current.Value.(string))

			if err != nil {
				state.mutex.Unlock()
				result := protocol.EncodeError("ERR value is not an integer or out of range")
				return &result
			}

			x += 1
			current.Value = strconv.Itoa(x)
		} else {
			// Otherwise create the key and give it the value 1
			x = 1

			value := rdb.ValueEntry{
				Key:    command.arguments[0],
				Value:  "1",
				Type:   rdb.TString,
				Expiry: nil,
			}

			state.values[command.arguments[0]] = &value
		}

		state.mutex.Unlock()

		result := protocol.EncodeInteger(x)
		return &result

	case "CONFIG":
		if len(command.arguments) == 2 && strings.ToUpper(command.arguments[0]) == "GET" {
			value, exists := state.config[command.arguments[1]]

			if exists {
				result := protocol.EncodeArray([]string{command.arguments[1], value})
				return &result
			} else {
				result := protocol.EncodeNullBulkString()
				return &result
			}
		} else {
			respondToBadCommand(conn, command)
			return nil
		}

	case "XADD":
		if len(command.arguments) < 2 {
			respondToBadCommand(conn, command)
			return nil
		}

		streamName := command.arguments[0]
		// TODO: Handle missing entryId
		rawEntryId := command.arguments[1]

		// Read pairs of arguments into properties for the entry
		properties := []rdb.StreamEntryProperty{}
		for i := 2; i+1 < len(command.arguments); i += 2 {
			properties = append(properties, rdb.StreamEntryProperty{
				Key:   command.arguments[i],
				Value: command.arguments[i+1],
			})
		}

		state.mutex.Lock()

		// Check that the stream key exists, and if not create it
		value, exists := state.values[streamName]

		if !exists {
			stream := rdb.Stream{
				Entries: []*rdb.StreamEntry{},
			}

			value = &rdb.ValueEntry{
				Key:    streamName,
				Value:  &stream,
				Type:   rdb.TStream,
				Expiry: nil,
			}
			state.values[value.Key] = value
		} else {
			// TODO: Validate it's a stream
		}

		// Now we know the value exists and is a stream
		stream := value.Value.(*rdb.Stream)

		entryId, err := rdb.EntryIdFromString(rawEntryId, stream)

		if err != nil {
			result := protocol.EncodeError(err.Error())
			state.mutex.Unlock()
			return &result
		}

		entry := rdb.StreamEntry{
			Id:         *entryId,
			Properties: properties,
		}

		// Validate the entryId is valid for the given stream
		err = entryId.ValidateAgainstStream(value.Value.(*rdb.Stream))
		if err != nil {
			state.mutex.Unlock()
			result := protocol.EncodeError(err.Error())
			return &result
		}

		// Finally append the validated entry into the stream.
		stream.Entries = append(stream.Entries, &entry)

		// Find watches for this stream and notify for change (sending the
		// stream name so watchers can easily identify the channel).
		for i := range state.watchers {
			*state.watchers[i] <- value
		}

		state.mutex.Unlock()

		result := protocol.EncodeBulkString(entry.Id.String())
		return &result

	case "XRANGE":
		if len(command.arguments) < 3 {
			respondToBadCommand(conn, command)
			return nil
		}

		streamName := command.arguments[0]
		rawStart := command.arguments[1]
		rawEnd := command.arguments[2]

		state.mutex.RLock()

		value, exists := state.values[streamName]

		if !exists {
			// TODO
			state.mutex.RUnlock()
			return nil
		} else if value.Type != rdb.TStream {
			// TODO
			state.mutex.RUnlock()
			return nil
		}

		stream := value.Value.(*rdb.Stream)
		// TODO: Handle errors

		// Parse the starting point, allowing "-" as from the beginning.
		var start *rdb.EntryId = nil
		if rawStart == "-" {
			start = &rdb.EntryId{
				MilliTime:      0,
				SequenceNumber: 0,
			}
		} else {
			start, _ = rdb.EntryIdFromString(rawStart, stream)
		}

		// Parse the ending point, allowing "+" as until the end
		var end *rdb.EntryId = nil
		if rawEnd == "+" {
			// Use max ints
			end = &rdb.EntryId{
				MilliTime:      int(^uint(0) >> 1),
				SequenceNumber: int(^uint(0) >> 1),
			}
		} else {
			end, _ = rdb.EntryIdFromString(rawEnd, stream)
		}

		resp := []string{}

		// TODO: Binary search for the starting point.
		for i := range stream.Entries {
			entry := stream.Entries[i]
			// If entry
			if entry.Id.MilliTime < start.MilliTime || (entry.Id.MilliTime == start.MilliTime && entry.Id.SequenceNumber < start.SequenceNumber) {
				continue
			}
			if entry.Id.MilliTime > end.MilliTime || (entry.Id.MilliTime == end.MilliTime && entry.Id.SequenceNumber > end.SequenceNumber) {
				break
			}

			resp = append(resp, encodeEntry(entry))
		}

		state.mutex.RUnlock()

		result := protocol.EncodeEncodedArray(resp)
		return &result

	case "XREAD":
		if len(command.arguments) < 3 {
			respondToBadCommand(conn, command)
			return nil
		}

		argumentOffset := 0
		blocking := -1

		if strings.ToUpper(command.arguments[0]) == "BLOCK" {
			argumentOffset += 2
			// TODO: Error handling
			blocking, _ = strconv.Atoi(command.arguments[1])
		}
		if strings.ToUpper(command.arguments[argumentOffset]) == "STREAMS" {
			argumentOffset += 1
		} else {
			respondToBadCommand(conn, command)
			return nil
		}

		state.mutex.RLock()

		pairs := (len(command.arguments) - argumentOffset) / 2

		// streamsResults holds all pairs of (stream id, entry results)
		streamsResults := []string{}
		streamNames := []string{}

		// Streams and starting points are send as (a, b, c, a-start, b-start, c-start)
		// So for given pair, the start is at i and i+pairCount (offset for the initial arguments)
		for i := 0; i < pairs; i++ {
			streamName := command.arguments[argumentOffset+i]
			streamNames = append(streamNames, streamName)
			rawStart := command.arguments[argumentOffset+i+pairs]

			log.Printf("XREAD %s %s", streamName, rawStart)

			value, exists := state.values[streamName]

			if !exists {
				// TODO
				state.mutex.RUnlock()
				return nil
			} else if value.Type != rdb.TStream {
				// TODO
				state.mutex.RUnlock()
				return nil
			}

			stream := value.Value.(*rdb.Stream)
			// TODO: Handle errors

			start, _ := rdb.EntryIdFromString(rawStart, stream)

			// Stream results holds the (entry-id, properties) pairs that have been
			// encoded into an Array string
			streamResults := []string{}

			// TODO: Binary search for the starting point.
			for i := range stream.Entries {
				entry := stream.Entries[i]
				// If entry
				if entry.Id.MilliTime < start.MilliTime || (entry.Id.MilliTime == start.MilliTime && entry.Id.SequenceNumber <= start.SequenceNumber) {
					continue
				}

				streamResults = append(streamResults, encodeEntry(entry))
			}

			if len(streamResults) > 0 {
				// result is constructed to hold the (stream-id, entries) pair, encoded as
				// an Array string.
				result := protocol.EncodeEncodedArray([]string{
					protocol.EncodeString(streamName),
					protocol.EncodeEncodedArray(streamResults),
				})

				streamsResults = append(streamsResults, result)
			}
		}

		// If we're blocking and no results were found, insert a channel to watch
		// for changes on.
		var watchChannel *ValueWatchChannel = nil
		if len(streamsResults) == 0 && blocking != -1 {
			c := make(ValueWatchChannel)
			watchChannel = &c
			state.watchers[conn.id] = watchChannel
		}

		state.mutex.RUnlock()

		// If we opened a watch channel, wait on that channel for changes
		if watchChannel != nil {
			// Create a dummy timeout channel and replace if blocking is set to non-zero.
			timeoutChannel := make(<-chan time.Time)
			if blocking > 0 {
				timeoutChannel = time.After(time.Duration(blocking) * time.Millisecond)
			}

			var result *rdb.ValueEntry = nil

			// Wait until we timeout, or a stream we're interested in is updated.
		BlockingLoop:
			for {
				select {
				case <-timeoutChannel:
					break BlockingLoop

				case value, ok := <-*watchChannel:
					if !ok {
						break BlockingLoop
					}
					if value != nil && slices.Contains(streamNames, value.Key) {
						result = value
						break BlockingLoop
					}
				}
			}

			// Unconditionally reacquire the lock to clean up channel
			state.mutex.RLock()

			close(*watchChannel)
			delete(state.watchers, conn.id)

			// If we found a result, add the latest value from that stream into the results.
			if result != nil {
				stream := result.Value.(*rdb.Stream)

				encodedEntry := encodeEntry(stream.Entries[len(stream.Entries)-1])

				newStreamResults := protocol.EncodeEncodedArray([]string{
					protocol.EncodeString(result.Key),
					protocol.EncodeEncodedArray([]string{encodedEntry}),
				})

				streamsResults = append(streamsResults, newStreamResults)
			}

			state.mutex.RUnlock()
		}

		if len(streamsResults) > 0 {
			// The final result is then the encoding of resp inside a top level array.
			result := protocol.EncodeEncodedArray(streamsResults)
			return &result
		} else {
			result := protocol.EncodeNullBulkString()
			return &result
		}

	case "TYPE":
		if len(command.arguments) < 1 {
			respondToBadCommand(conn, command)
			return nil
		}

		state.mutex.RLock()
		value, exists := state.values[command.arguments[0]]

		if exists {
			result := protocol.EncodeString(rdb.TypeToString(value.Type))
			state.mutex.RUnlock()
			return &result
		} else {
			state.mutex.RUnlock()

			result := protocol.EncodeString("none")
			return &result
		}

	default:
		respondToBadCommand(conn, command)
		return nil
	}
}

func handleConnection(conn net.Conn, connId int, state *ServerState) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	connState := ConnState{
		id:          connId,
		conn:        conn,
		isBuffering: false,
		buffer:      []*Command{},
		addr:        conn.RemoteAddr().String(),
	}

	for {
		// Peek the first character to determine the datatype being sent
		command, err := receiveCommand(reader)

		if checkReadError(err, connState.addr) {
			return
		}

		// Meta commands dealing with Transactions
		if command.name == "MULTI" {
			connState.isBuffering = true
			conn.Write([]byte(protocol.EncodeString("OK")))
		} else if command.name == "DISCARD" {
			if !connState.isBuffering {
				conn.Write([]byte(protocol.EncodeError("ERR DISCARD without MULTI")))
				return
			}

			connState.buffer = []*Command{}
			connState.isBuffering = false
			conn.Write([]byte(protocol.EncodeString("OK")))
		} else if command.name == "EXEC" {
			if !connState.isBuffering {
				conn.Write([]byte(protocol.EncodeError("ERR EXEC without MULTI")))
				return
			}

			// Iterate through the buffer and store the results of each command in sequence
			var results = []string{}
			for i := range connState.buffer {
				results = append(results, *processCommand(&connState, connState.buffer[i], state))
			}

			// As the return values of processCommand are themselves already encoded, just encode
			// the outer Array wrapper to return the values.
			conn.Write([]byte(protocol.EncodeEncodedArray(results)))

			connState.buffer = []*Command{}
			connState.isBuffering = false
		} else if connState.isBuffering {
			// Queue commands while buffering
			connState.buffer = append(connState.buffer, command)
			conn.Write([]byte(protocol.EncodeString("QUEUED")))
		} else {
			// Otherwise just execute as we receive.
			result := processCommand(&connState, command, state)
			if result != nil {
				conn.Write([]byte(*result))
			}
		}
	}
}
