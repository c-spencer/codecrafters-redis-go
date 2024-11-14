package commands

import (
	"errors"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/domain"
	"github.com/codecrafters-io/redis-starter-go/internal/protocol"
)

// ...existing code...

type WaitHandler struct {
	BaseHandler

	numReplicas int
	timeout     time.Duration
}

func NewWaitHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) != 2 {
		return nil, errors.New("ERR wrong number of arguments for 'WAIT' command")
	}

	numReplicas, err := strconv.Atoi(cmd.Arguments[0])
	if err != nil {
		return nil, errors.New("ERR invalid number of replicas")
	}

	timeoutMillis, err := strconv.Atoi(cmd.Arguments[1])
	if err != nil {
		return nil, errors.New("ERR invalid timeout")
	}

	return &WaitHandler{
		numReplicas: numReplicas,
		timeout:     time.Duration(timeoutMillis) * time.Millisecond,
	}, nil
}

func (h *WaitHandler) Execute(state domain.State, reply func(string)) error {
	//replicatedCount := state.WaitForReplication(h.numReplicas, h.timeout)
	reply(protocol.EncodeInteger(0))
	return nil
}

func (h *WaitHandler) Mutability() CommandMutability {
	return CmdRead
}
