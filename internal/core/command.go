package core

import (
	"errors"
	"strings"
)

type Command struct {
	Cmd  string
	Args []string
}

func ParseCommand(cmd string) (*Command, error) {
	tokens := strings.Split(cmd, " ")
	if len(tokens) == 0 {
		return nil, errors.New("invalid command: Please enter a command")
	}
	return &Command{
		Cmd:  tokens[0],
		Args: tokens[1:],
	}, nil
}
