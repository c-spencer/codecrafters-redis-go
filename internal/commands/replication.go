package commands

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/domain"
	"github.com/codecrafters-io/redis-starter-go/internal/protocol"
)

// WAIT numreplicas timeout

type WaitHandler struct {
	BaseHandler

	numReplicas int
	timeout     time.Duration
	wg          *sync.WaitGroup
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

	wg := sync.WaitGroup{}
	wg.Add(1)

	return &WaitHandler{
		BaseHandler: BaseHandler{command: cmd},

		numReplicas: numReplicas,
		timeout:     time.Duration(timeoutMillis) * time.Millisecond,
		wg:          &wg,
	}, nil
}

func (h *WaitHandler) Execute(state domain.State, reply func(string)) error {
	connOffset := state.ConnectionOffset()

	// Synchronously check if we already have enough replicas
	replicaCount := state.ReplicasAtOffset(connOffset)
	if replicaCount >= h.numReplicas {
		reply(protocol.EncodeInteger(replicaCount))
		h.wg.Done()
	} else {
		// Otherwise wait for more replicas, blocking the client until we respond.
		state.WaitForReplicas(connOffset, h.numReplicas, h.timeout, func(count int) {
			reply(protocol.EncodeInteger(count))
			h.wg.Done()
		})
	}

	return nil
}

func (h *WaitHandler) Mutability() CommandMutability {
	return CmdRead
}

func (h *WaitHandler) Wait() {
	h.wg.Wait()
}

// REPLCONF command entrypoint

type ReplConfMode string

func NewReplConfHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) < 1 {
		return nil, errors.New("ERR wrong number of arguments for 'REPLCONF' command")
	}

	switch ReplConfMode(cmd.Arguments[0]) {
	case "GETACK":
		return NewReplConfGetAckHandler(cmd)
	case "ACK":
		return NewReplConfAckHandler(cmd)
	default:
		return nil, errors.New("ERR unknown REPLCONF mode")
	}
}

// REPLCONF GETACK offset

type ReplConfGetAckHandler struct {
	BaseHandler

	offset int
}

func NewReplConfGetAckHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) != 2 || cmd.Arguments[0] != "GETACK" {
		return nil, errors.New("ERR wrong number of arguments for 'REPLCONF GETACK' command")
	}

	var offset = -1
	if cmd.Arguments[1] != "*" {
		return nil, errors.New("ERR specific GETACK offsets NYI")
	}

	return &ReplConfGetAckHandler{
		BaseHandler: BaseHandler{command: cmd},

		offset: offset,
	}, nil
}

func (h *ReplConfGetAckHandler) Execute(state domain.State, reply func(string)) error {
	masterReplOffset, _ := state.ReplicationInfo().GetInt("masterReplOffset")
	reply(protocol.EncodeArray([]string{"REPLCONF", "ACK", strconv.Itoa(masterReplOffset)}))
	return nil
}

func (h *ReplConfGetAckHandler) Mutability() CommandMutability {
	return CmdWrite
}

// REPLCONF ACK offset

type ReplConfAckHandler struct {
	BaseHandler

	offset int
}

func NewReplConfAckHandler(cmd *Command) (Handler, error) {
	if len(cmd.Arguments) != 2 || cmd.Arguments[0] != "ACK" {
		return nil, errors.New("ERR wrong number of arguments for 'REPLCONF ACK' command")
	}

	offset, err := strconv.Atoi(cmd.Arguments[1])
	if err != nil {
		return nil, errors.New("ERR invalid offset")
	}

	return &ReplConfAckHandler{
		BaseHandler: BaseHandler{command: cmd},

		offset: offset,
	}, nil
}

func (h *ReplConfAckHandler) Execute(state domain.State, reply func(string)) error {
	state.SetConnectionOffset(h.offset)
	return nil
}

func (h *ReplConfAckHandler) Mutability() CommandMutability {
	return CmdRead
}
