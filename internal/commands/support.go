package commands

import (
	"fmt"
	"strconv"
	"strings"
)

/// This file contains support functions that are used by multiple commands.

// Parse the integer argument for an option to a command, or an error if the argument is not an integer
// or the argument is missing.
//
// The slice should contain the argument name and all subsequent arguments. Contractually the args slice
// must have at least one element (the option name being parsed).
func parseIntegerArgument(commandName string, args []string) (int64, error) {
	optionName := strings.ToUpper(args[0])

	if len(args) < 2 {
		return 0, fmt.Errorf("ERR wrong number of arguments for %s option '%s'", commandName, optionName)
	}
	i, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR argument for %s option '%s' to be integer", commandName, optionName)
	}

	return i, nil
}
