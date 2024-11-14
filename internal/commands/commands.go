package commands

import (
	"errors"

	"github.com/codecrafters-io/redis-starter-go/internal/domain"
)

type CommandMutability uint8

const (
	CmdRead CommandMutability = 1 << iota
	CmdWrite
)

func (cm CommandMutability) IsRead() bool {
	return cm&CmdRead != 0
}
func (cm CommandMutability) IsWrite() bool {
	return cm&CmdWrite != 0
}
func (cm CommandMutability) IsStateless() bool {
	return cm == 0
}

// Command represents a parsed Redis command.
//
// Name will be already normalized to uppercase, and Arguments will be the list
// of arguments passed to the command, without any other processing.
type Command struct {
	Name      string
	Arguments []string
}

// Handler is the interface that must be implemented by all command handlers.
//
// It allows to execute the command on a given server state, and introspect the
// mutability requirements of the command, for the purposes of replication and
// other features.
type Handler interface {
	// Return the original command that this handler is implementing
	Command() *Command

	// Execute the command on the given server state, and reply with the result.
	Execute(state domain.State, reply func(string)) error

	// Return the mutability requirements of the command
	Mutability() CommandMutability

	// Wait for this handler to finish
	// Only relevant for possibly blocking commands, e.g. XREAD
	Wait()
}

// Provide a BaseHandler providing a default (empty) implementation for Wait(),
// as it is not relevant for most commands.
type BaseHandler struct {
	command *Command
}

func (h *BaseHandler) Wait() {}
func (h *BaseHandler) Command() *Command {
	return h.command
}

// Map from command name to handler factory functions
var handlers = map[string]func(*Command) (Handler, error){
	// Utility commands
	"PING":   NewPingHandler,
	"ECHO":   NewEchoHandler,
	"CONFIG": NewConfigHandler,
	"INFO":   NewInfoHandler,

	// Key-value commands
	"GET":  NewGetHandler,
	"SET":  NewSetHandler,
	"INCR": NewIncrHandler,

	// Key utility commands
	"KEYS": NewKeysHandler,
	"TYPE": NewTypeHandler,

	// Streams commands
	"XADD":   NewXAddHandler,
	"XREAD":  NewXReadHandler,
	"XRANGE": NewXRangeHandler,

	// Replication commands
	"WAIT":     NewWaitHandler,
	"REPLCONF": NewReplConfHandler,
}

func (c *Command) Handler() (Handler, error) {
	handlerFunc, exists := handlers[c.Name]

	if exists {
		return handlerFunc(c)
	} else {
		return nil, errors.New("ERR unknown command '" + c.Name + "'")
	}
}
