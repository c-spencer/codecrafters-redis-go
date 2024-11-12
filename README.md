[![progress-banner](https://backend.codecrafters.io/progress/redis/ba182879-93d9-4e71-bc9b-8799e0c1509d)](https://app.codecrafters.io/users/c-spencer)

This is a starting point for Go solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

In this challenge, you'll build a toy Redis clone that's capable of handling
basic commands like `PING`, `SET` and `GET`. Along the way we'll learn about
event loops, the Redis protocol and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Status

For the given codecrafters course sections

- [x] Basics
- [ ] Replication
- [x] Persistence
- [x] Transactions
- [x] Streams

# Todo list

Todo list of things that pass the tests but aren't fully fleshed out.

- Locking for EXEC should be done at the top level rather than per-command.
- Implement a reaper goroutine and channel that handles removing expired keys. Each conn can send new expiries to it via a channel, and it can track the queue of expiries itself.
- Checking of validity for command arguments etc is very slapdash.
- Error handling elided in various places.
- `processCommand` could do with breaking into pieces and refactoring.
