package commands

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/domain"
	"github.com/codecrafters-io/redis-starter-go/internal/protocol"
	"github.com/codecrafters-io/redis-starter-go/internal/rdb"
)

// PING [message]
// TODO implement message return

type PingHandler struct {
	BaseHandler
}

func NewPingHandler(cmd *Command) (Handler, error) {
	return &PingHandler{
		BaseHandler: BaseHandler{command: cmd},
	}, nil
}

func (h *PingHandler) Execute(state domain.State, reply func(string)) error {
	reply(protocol.EncodeString("PONG"))
	return nil
}

func (h *PingHandler) Mutability() CommandMutability {
	return CmdRead
}

// The ExecHandler is a special handler that executes a list of other handlers
// in sequence, returning the concatenation of all responses. This is used to
// implement the MULTI/EXEC transaction commands.

type ExecHandler struct {
	handlers []Handler
}

func NewExecHandler(handlers []Handler) Handler {
	return &ExecHandler{
		handlers: handlers,
	}
}

func (h *ExecHandler) Command() *Command {
	log.Fatalf("ExecHandler.Command() not implemented")
	return nil
}
func (h *ExecHandler) Wait() {}

func (h *ExecHandler) Execute(state domain.State, reply func(string)) error {
	var responses = []string{}

	recordResponse := func(response string) {
		responses = append(responses, response)
	}

	for _, handler := range h.handlers {
		err := handler.Execute(state, recordResponse)

		if err != nil {
			responses = append(responses, protocol.EncodeError(err.Error()))
		}
	}

	reply(protocol.EncodeEncodedArray(responses))
	return nil
}

func (h *ExecHandler) Mutability() CommandMutability {
	// TODO: Check all handlers
	return CmdRead | CmdWrite
}

// ECHO message

type EchoHandler struct {
	BaseHandler

	message string
}

func NewEchoHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'ECHO' command")
	}
	return &EchoHandler{
		message: cmd.Arguments[0],
	}, nil
}

func (h *EchoHandler) Execute(state domain.State, reply func(string)) error {
	reply(protocol.EncodeString(h.message))
	return nil
}

func (h *EchoHandler) Mutability() CommandMutability {
	return CmdRead
}

// CONFIG command entrypoint

func NewConfigHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) < 1 {
		return nil, errors.New("ERR wrong number of arguments for 'CONFIG' command")
	}

	switch cmd.Arguments[0] {
	case "GET":
		return NewConfigGetHandler(cmd)
	default:
		return nil, errors.New("ERR unknown CONFIG mode")
	}
}

// CONFIG GET parameter [parameter ...]
// https://redis.io/commands/config-get/
//
// TODO: Implement multiple parameters

type ConfigGetHandler struct {
	BaseHandler

	parameter string
}

func NewConfigGetHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) != 2 || cmd.Arguments[0] != "GET" {
		return nil, errors.New("ERR wrong number of arguments for 'CONFIG' command")
	}
	return &ConfigGetHandler{
		BaseHandler: BaseHandler{command: cmd},

		parameter: cmd.Arguments[1],
	}, nil
}

func (h *ConfigGetHandler) Execute(state domain.State, reply func(string)) error {
	value, exists := state.Config().Get(h.parameter)

	if exists {
		reply(protocol.EncodeArray([]string{h.parameter, value}))
	} else {
		reply(protocol.EncodeNullBulkString())
	}

	return nil
}

func (h *ConfigGetHandler) Mutability() CommandMutability {
	return CmdRead
}

// TYPE key
// https://redis.io/commands/type/

type TypeHandler struct {
	BaseHandler

	key string
}

func NewTypeHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) != 1 {
		return nil, errors.New("ERR wrong number of arguments for 'TYPE' command")
	}

	return &TypeHandler{
		BaseHandler: BaseHandler{command: cmd},

		key: cmd.Arguments[0],
	}, nil
}

func (h *TypeHandler) Execute(state domain.State, reply func(string)) error {
	value, exists := state.Get(h.key)

	if exists {
		reply(protocol.EncodeString(rdb.TypeToString(value.Type)))
	} else {
		reply(protocol.EncodeString("none"))
	}

	return nil
}

func (h *TypeHandler) Mutability() CommandMutability {
	return CmdRead
}

// INFO [section [section ...]]
// https://redis.io/commands/info/
//
// TODO: Implement more sections, and optional section argument

type InfoHandler struct {
	BaseHandler

	section string
}

func NewInfoHandler(cmd *Command) (Handler, error) {
	section := ""
	if len(cmd.Arguments) > 0 {
		section = cmd.Arguments[0]
	}

	return &InfoHandler{
		BaseHandler: BaseHandler{command: cmd},

		section: section,
	}, nil
}

func (h *InfoHandler) Execute(state domain.State, reply func(string)) error {
	if h.section == "" || h.section == "replication" {
		replicationInfo := state.ReplicationInfo()

		info := strings.Join(
			[]string{
				fmt.Sprintf("role:%s", first(replicationInfo.Get("role"))),
				fmt.Sprintf("master_replid:%s", first(replicationInfo.Get("masterReplId"))),
				fmt.Sprintf("master_repl_offset:%d", first(replicationInfo.GetInt("masterReplOffset"))),
			}, "\r\n",
		)

		reply(protocol.EncodeBulkString(info))
	} else {
		reply(protocol.EncodeError("ERR unknown section"))
	}

	return nil
}

func (h *InfoHandler) Mutability() CommandMutability {
	return CmdRead
}
