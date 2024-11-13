package commands

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/domain"
	"github.com/codecrafters-io/redis-starter-go/internal/protocol"
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
)

// GET key
// https://redis.io/docs/latest/commands/get/

type GetHandler struct {
	BaseHandler
	key string
}

func NewGetHandler(cmd *Command) (Handler, error) {
	return &GetHandler{
		BaseHandler: BaseHandler{command: cmd},

		key: cmd.Arguments[0],
	}, nil
}

// Implement Handler interface methods

func (h *GetHandler) Execute(state domain.State, reply func(string)) error {
	value, exists := state.Get(h.key)

	if exists {
		reply(protocol.EncodeBulkString(value.Value.(string)))
	} else {
		reply(protocol.EncodeNullBulkString())
	}

	return nil
}

func (h *GetHandler) Mutability() CommandMutability {
	return CmdRead
}

// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
//   EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
// https://redis.io/docs/latest/commands/set/

type SetHandler struct {
	BaseHandler

	key   string
	value string

	nx        bool
	xx        bool
	get       bool
	duration  *time.Duration
	expiresAt *time.Time
	keepTTL   bool
}

func NewSetHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) < 2 {
		return nil, errors.New("ERR wrong number of arguments for 'SET' command")
	}

	handler := SetHandler{
		BaseHandler: BaseHandler{command: cmd},

		key:   cmd.Arguments[0],
		value: cmd.Arguments[1],
	}

	// Parse options starting from the 3rd argument
	for i := 2; i < len(cmd.Arguments); i++ {
		// Normalize argument to uppercase
		arg := strings.ToUpper(cmd.Arguments[i])

		switch arg {
		case "NX":
			handler.nx = true
		case "XX":
			handler.xx = true
		case "GET":
			handler.get = true
		case "KEEPTTL":
			handler.keepTTL = true

		case "EX", "PX", "EXAT", "PXAT":
			intarg, err := parseIntegerArgument("SET", cmd.Arguments[i:])
			if err != nil {
				return nil, err
			}

			switch arg {
			case "EX":
				expiry := time.Duration(intarg) * time.Second
				handler.duration = &expiry
			case "PX":
				expiry := time.Duration(intarg) * time.Millisecond
				handler.duration = &expiry
			case "EXAT":
				expiry := time.Unix(intarg, 0)
				handler.expiresAt = &expiry
			case "PXAT":
				expiry := time.UnixMilli(intarg)
				handler.expiresAt = &expiry
			}

			// Skip the next argument
			i++
		}
	}

	return &handler, nil
}

func (h *SetHandler) Execute(state domain.State, reply func(string)) error {
	// Implementation
	value, exists := state.Get(h.key)

	// If NX and it exists, return nil
	if h.nx && exists {
		reply(protocol.EncodeNullBulkString())
		return nil
	}

	// If XX and it doesn't exist, return nil
	if h.xx && !exists {
		reply(protocol.EncodeNullBulkString())
		return nil
	}

	// Keep a copy of the previous value, in the case we need to return it
	previousValue := value

	var expiry *time.Time = nil

	if h.duration != nil {
		expiryTime := time.Now().Add(*h.duration)
		expiry = &expiryTime
	} else if h.expiresAt != nil {
		expiry = h.expiresAt
	} else if h.keepTTL && exists {
		expiry = value.Expiry
	}

	// Set the new value
	// For simplicity and safety, we'll always overwrite the value. This is not
	// strictly necessary, but it simplifies the implementation.
	state.Set(&rdb.ValueEntry{
		Key:    h.key,
		Value:  h.value,
		Type:   rdb.TString,
		Expiry: expiry,
	})

	if h.get && exists {
		reply(protocol.EncodeBulkString(previousValue.Value.(string)))
		return nil
	} else {
		reply(protocol.EncodeString("OK"))
		return nil
	}
}

func (h *SetHandler) Mutability() CommandMutability {
	return CmdRead | CmdWrite
}

// KEYS pattern
// https://redis.io/docs/latest/commands/keys/

type KeysHandler struct {
	BaseHandler

	pattern string
}

func NewKeysHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'KEYS' command")
	}

	return &KeysHandler{
		BaseHandler: BaseHandler{command: cmd},

		pattern: cmd.Arguments[0],
	}, nil
}

func (h *KeysHandler) Execute(state domain.State, reply func(string)) error {
	keys := state.Keys(h.pattern)

	reply(protocol.EncodeArray(keys))
	return nil
}

func (h *KeysHandler) Mutability() CommandMutability {
	return CmdRead
}

// INCR key

type IncrHandler struct {
	BaseHandler

	key string
}

func NewIncrHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'INCR' command")
	}

	return &IncrHandler{
		BaseHandler: BaseHandler{command: cmd},

		key: cmd.Arguments[0],
	}, nil
}

func (h *IncrHandler) Command() *Command {
	return &Command{
		Name:      "INCR",
		Arguments: []string{h.key},
	}
}

func (h *IncrHandler) Execute(state domain.State, reply func(string)) error {
	value, exists := state.Get(h.key)

	var x int
	var err error

	if exists {
		// TODO: Check type is string
		x, err = strconv.Atoi(value.Value.(string))
		if err != nil {
			return errors.New("ERR value is not an integer or out of range")
		}
		x += 1
	} else {
		x = 1
	}

	state.Set(&rdb.ValueEntry{
		Key:    h.key,
		Value:  strconv.Itoa(x),
		Type:   rdb.TString,
		Expiry: nil,
	})

	reply(protocol.EncodeInteger(x))
	return nil
}

func (h *IncrHandler) Mutability() CommandMutability {
	return CmdRead | CmdWrite
}
