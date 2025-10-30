package core

import (
	"fmt"
	"strings"

	"github.com/iscoreyagain/GoCask/internal"
)

var bc *internal.BitCask

func SetBitCask(bitcask *internal.BitCask) {
	bc = bitcask
}

// ExecuteAndResponse executes a command and returns the response
func ExecuteAndResponse(cmd *Command) string {
	switch strings.ToUpper(cmd.Cmd) {
	case "GET", "PUT":
		return cmdGET(cmd.Args)
	case "SET":
		return cmdSET(cmd.Args)
	case "DEL", "DELETE":
		return cmdDEL(cmd.Args)
	case "EXISTS":
		return cmdEXISTS(cmd.Args)
	case "KEYS":
		return cmdKEYS(cmd.Args)
	case "SYNC":
		return cmdSYNC(cmd.Args)
	case "PING":
		return cmdPING(cmd.Args)
	case "INFO":
		return cmdINFO(cmd.Args)
	default:
		return fmt.Sprintf("-ERR unknown command '%s'", cmd.Cmd)
	}
}

func cmdGET(args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'GET' command"
	}

	key := args[0]
	value, err := bc.Get(key)
	if err != nil {
		return "$-1"
	}

	return fmt.Sprintf("$%d\r\n%s", len(value), value)
}

func cmdSET(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'SET' command"
	}

	key := args[0]
	value := strings.Join(args[1:], " ")

	if err := bc.Put(key, value); err != nil {
		return fmt.Sprintf("-ERR %v", err)
	}

	return "+OK"
}

func cmdDEL(args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'DEL' command"
	}

	key := args[0]
	err := bc.Delete(key)
	if err != nil {
		return ":0"
	}

	return ":1"
}

func cmdEXISTS(args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'EXISTS' command"
	}

	key := args[0]
	if _, exist := bc.KeyDir[key]; !exist {
		return ":0"
	}
	return ":1"
}

func cmdKEYS(args []string) string {
	if len(args) != 0 {
		return "-ERR wrong number of arguments for 'KEYS' command"
	}

	var keys []string
	for key := range bc.KeyDir {
		keys = append(keys, key)
	}

	if len(keys) == 0 {
		return "*0\r\n"
	}

	result := fmt.Sprintf("*%d\r\n", len(keys))
	for _, key := range keys {
		result += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
	}
	return result
}

func cmdPING(args []string) string {
	if len(args) == 0 {
		return "+PONG"
	}
	return fmt.Sprintf("$%d\r\n%s", len(args[0]), args[0])
}

func cmdINFO(args []string) string {
	if len(args) != 0 {
		return "-ERR wrong number of arguments for 'INFO' command"
	}
	bc.Mu.RLock()
	info := fmt.Sprintf("# Server\r\nkeys=%d\r\nfiles=%d\r\n",
		len(bc.KeyDir), len(bc.Files))
	bc.Mu.RUnlock()

	return fmt.Sprintf("$%d\r\n%s", len(info), info)
}

func cmdSYNC(args []string) string {
	if len(args) != 0 {
		return "-ERR wrong number of arguments for 'SYNC' command"
	}
	if err := bc.Sync(); err != nil {
		return fmt.Sprintf("-ERR %v", err)
	}
	return "+OK"
}
