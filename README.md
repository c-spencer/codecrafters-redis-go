[![progress-banner](https://backend.codecrafters.io/progress/redis/ba182879-93d9-4e71-bc9b-8799e0c1509d)](https://app.codecrafters.io/users/c-spencer)

This is a starting point for Go solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

In this challenge, you'll build a toy Redis clone that's capable of handling
basic commands like `PING`, `SET` and `GET`. Along the way we'll learn about
event loops, the Redis protocol and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Status

For the given codecrafters course sections.

- [x] Basics
    - `GET`/`SET` implementations
    - Key expiry
- [x] Replication
    - `--replicaof` CLI argument
    - Master<>Slave asynchronous replication and initial database transfer.
    - `WAIT` implementation
    - Partial `REPLCONF` implementation (to meet the tests)
- [x] Persistence
    - Hydration of basic K/V databases from disk.
    - `KEYS` implementation
- [x] Transactions
    - `INCR` implementation
    - `MULTI`/`EXEC`/`DISCARD` implementations
- [x] Streams
    - `TYPE` implementation
    - Basic `XADD`/`XREAD`/`XRANGE` implementations, including id generation, validation and blocking.

Implemented over the course of a few days, starting from near zero experience with Go.

# Architectural Overview

This server has been structured to use three distinct classes of goroutine.

- The executor goroutine, a singleton which manages the main loop of executing and responding to commands.
- The replication goroutine, an optional singleton which connects to a master and passes replication state to the executor goroutine.
- Connection goroutines, which are spawned for each connection to the server, including connections from replicas. These handle initial command parsing and transactions, and then pass off to the main executor goroutine via a channel.

Commands are implemented a generic `Command` struct, which is then wrapped by a `Handler` struct which is constructed and validates the arguments for that command. The API between `Handler` and state is defined in `internal/domain` as the `State` interface.

The `internal/protocol` package implements methods to assist in reading/writing RESP 2.0 messages. The `internal/rdb` package implements the persistence loading, and is likely the most brittle part as it has only just enough to pass the codecrafters test suites.

Values are represented by the `rdb.ValueEntry` struct, which again is relatively brittle and is shared between data loading and runtime to simplify logic of hydrating databases.

## Todo list

Todo list of things that pass the tests but aren't fully fleshed out.

- Improve performance of key expiry. Currently all keys are scanned periodically.
- Eliminate any places error handling was elided during implementation.
- The connection replOffset passing to allow WAIT/REPLCONF ACK to work is currently very brittle. Rework to add an explicit connection state to be passed into Handlers?
- Eliminate remaining special casing of commands which were glossed over in codecrafters, e.g. the other REPLCONF commands issued during handshake.
- Other commands/options that were elided as being not part of the codecrafters tests.
- In-repo tests for the main server loop, replication, etc.
- Implement partial database synchronisation and failover.
- Implement writing database state to disk.
- Add proper structured logging
