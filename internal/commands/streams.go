package commands

import (
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/domain"
	"github.com/codecrafters-io/redis-starter-go/internal/protocol"
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
)

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

// XADD key [NOMKSTREAM] [<MAXLEN | MINID> [= | ~] threshold
//   [LIMIT count]] <* | id> field value [field value ...]
// https://redis.io/commands/xadd/
// TODO: Implement NOMKSTREAM, MAXLEN, MINID, LIMIT.. and more

type XAddHandler struct {
	BaseHandler

	streamName string
	rawEntryId string
	properties []rdb.StreamEntryProperty
}

func NewXAddHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) < 3 {
		return nil, errors.New("ERR wrong number of arguments for 'XADD' command")
	}

	streamName := cmd.Arguments[0]
	rawEntryId := cmd.Arguments[1]

	properties := []rdb.StreamEntryProperty{}
	for i := 2; i+1 < len(cmd.Arguments); i += 2 {
		properties = append(properties, rdb.StreamEntryProperty{
			Key:   cmd.Arguments[i],
			Value: cmd.Arguments[i+1],
		})
	}

	return &XAddHandler{
		BaseHandler: BaseHandler{command: cmd},

		streamName: streamName,
		rawEntryId: rawEntryId,
		properties: properties,
	}, nil
}

func (h *XAddHandler) Execute(state domain.State, reply func(string)) error {
	value, exists := state.Get(h.streamName)

	if !exists {
		stream := rdb.Stream{
			Entries: []*rdb.StreamEntry{},
		}

		value = &rdb.ValueEntry{
			Key:    h.streamName,
			Value:  &stream,
			Type:   rdb.TStream,
			Expiry: nil,
		}
	} else if value.Type != rdb.TStream {
		return errors.New("ERR wrong type of key")
	}

	stream := value.Value.(*rdb.Stream)

	entryId, err := rdb.EntryIdFromString(h.rawEntryId, stream)
	if err != nil {
		return err
	}

	entry := rdb.StreamEntry{
		Id:         *entryId,
		Properties: h.properties,
	}

	err = entryId.ValidateAgainstStream(stream)
	if err != nil {
		return err
	}

	stream.Entries = append(stream.Entries, &entry)

	// Always re-assign the value to the state, as the stream may have been created
	// and this also triggers the subscription mechanism.
	state.Set(value)

	reply(protocol.EncodeBulkString(entry.Id.String()))
	return nil
}

func (h *XAddHandler) Mutability() CommandMutability {
	return CmdRead | CmdWrite
}

// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
// https://redis.io/commands/xread/
// TODO: Implement COUNT, reimplement BLOCK

type XReadHandler struct {
	BaseHandler

	blocking    int
	streamNames []string
	startIds    []string
	wg          *sync.WaitGroup
}

func NewXReadHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) < 3 {
		return nil, errors.New("ERR wrong number of arguments for 'XREAD' command")
	}

	argumentOffset := 0
	blocking := -1

	if strings.ToUpper(cmd.Arguments[0]) == "BLOCK" {
		argumentOffset += 2
		// TODO: Error handling
		blocking, _ = strconv.Atoi(cmd.Arguments[1])
	}
	if strings.ToUpper(cmd.Arguments[argumentOffset]) == "STREAMS" {
		argumentOffset += 1
	} else {
		return nil, errors.New("ERR wrong number of arguments for 'XREAD' command")
	}

	pairs := (len(cmd.Arguments) - argumentOffset) / 2
	streamNames := make([]string, pairs)
	startIds := make([]string, pairs)

	for i := 0; i < pairs; i++ {
		streamNames[i] = cmd.Arguments[argumentOffset+i]
		startIds[i] = cmd.Arguments[argumentOffset+i+pairs]
	}

	wg := sync.WaitGroup{}
	if blocking >= 0 {
		wg.Add(1)
	}

	return &XReadHandler{
		BaseHandler: BaseHandler{command: cmd},

		blocking:    blocking,
		streamNames: streamNames,
		startIds:    startIds,
		wg:          &wg,
	}, nil
}

func (h *XReadHandler) Execute(state domain.State, reply func(string)) error {
	// streamsResults holds all pairs of (stream id, entry results)
	streamsResults := []string{}

	for i, streamName := range h.streamNames {
		rawStart := h.startIds[i]

		// If the start is '$', skip here so the later blocking logic can take over.
		if rawStart == "$" {
			continue
		}

		value, exists := state.Get(streamName)

		if !exists {
			continue
		} else if value.Type != rdb.TStream {
			return errors.New("ERR wrong type of key")
		}

		stream := value.Value.(*rdb.Stream)

		start, _ := rdb.EntryIdFromString(rawStart, stream)

		// Stream results holds the (entry-id, properties) pairs that have been
		// encoded into an Array string
		streamResults := []string{}

		// TODO: Binary search for the starting point.
		for _, entry := range stream.Entries {
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

	if len(streamsResults) == 0 && h.blocking >= 0 {
		// If no stream results were found, and blocking is enabled, we need to subscribe to the streams
		// and wait for new entries to arrive. This is done via the Subscribe method on the state.
		//
		// The client connection will be blocked by the wait group until the callback is called.

		state.Subscribe(h.streamNames, h.blocking, func(value *rdb.ValueEntry) {
			if value == nil {
				reply(protocol.EncodeNullBulkString())
			} else {
				stream := value.Value.(*rdb.Stream)

				// Stream results holds the (entry-id, properties) pairs that have been
				// encoded into an Array string
				encodedEntry := encodeEntry(stream.Entries[len(stream.Entries)-1])
				streamResults := protocol.EncodeEncodedArray([]string{
					protocol.EncodeString(value.Key),
					protocol.EncodeEncodedArray([]string{encodedEntry}),
				})
				reply(protocol.EncodeEncodedArray([]string{streamResults}))
			}

			h.wg.Done()
		})

		// Return nil so nothing is sent back to the client and the executor can continue.
		return nil
	} else if h.blocking >= 0 {
		h.wg.Done()
	}

	if len(streamsResults) > 0 {
		// The final result is then the encoding of resp inside a top level array.
		reply(protocol.EncodeEncodedArray(streamsResults))
	} else {
		reply(protocol.EncodeNullBulkString())
	}

	return nil
}

func (h *XReadHandler) Mutability() CommandMutability {
	return CmdRead
}

func (h *XReadHandler) Wait() {
	h.wg.Wait()
}

// XRANGE key start end [COUNT count]
// https://redis.io/commands/xrange/
// TODO: Implement COUNT

type XRangeHandler struct {
	BaseHandler

	streamName string
	startId    string
	endId      string
}

func NewXRangeHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) != 3 {
		return nil, errors.New("ERR wrong number of arguments for 'XRANGE' command")
	}

	return &XRangeHandler{
		BaseHandler: BaseHandler{command: cmd},

		streamName: cmd.Arguments[0],
		startId:    cmd.Arguments[1],
		endId:      cmd.Arguments[2],
	}, nil
}

func (h *XRangeHandler) Execute(state domain.State, reply func(string)) error {
	value, exists := state.Get(h.streamName)

	if !exists {
		reply(protocol.EncodeNullBulkString())
		return nil
	} else if value.Type != rdb.TStream {
		return errors.New("ERR wrong type of key")
	}

	stream := value.Value.(*rdb.Stream)

	// Use nil to indicate the start and end are unbounded.
	var start *rdb.EntryId = nil
	var end *rdb.EntryId = nil
	var err error = nil

	if h.startId != "-" {
		start, err = rdb.EntryIdFromString(h.startId, stream)
		if err != nil {
			return err
		}
	}

	if h.endId != "+" {
		end, err = rdb.EntryIdFromString(h.endId, stream)
		if err != nil {
			return err
		}
	}

	resp := []string{}

	// TODO: Binary search for the starting point.
	for _, entry := range stream.Entries {
		if start != nil && (entry.Id.MilliTime < start.MilliTime ||
			(entry.Id.MilliTime == start.MilliTime && entry.Id.SequenceNumber < start.SequenceNumber)) {
			continue
		}
		if end != nil && (entry.Id.MilliTime > end.MilliTime ||
			(entry.Id.MilliTime == end.MilliTime && entry.Id.SequenceNumber > end.SequenceNumber)) {
			break
		}

		resp = append(resp, encodeEntry(entry))
	}

	reply(protocol.EncodeEncodedArray(resp))
	return nil
}

func (h *XRangeHandler) Mutability() CommandMutability {
	return CmdRead
}
