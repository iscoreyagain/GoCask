package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/iscoreyagain/GoCask/internal/config"
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	addr   string
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial(config.Protocol, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		addr:   addr,
	}, nil
}

func (c *Client) SendCommand(cmd string) (string, error) {
	// Send command
	_, err := c.writer.WriteString(cmd + "\n")
	if err != nil {
		return "", err
	}
	c.writer.Flush()

	// Read response
	response, err := c.reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(response), nil
}

func (c *Client) ReadBulkString(firstLine string) (string, error) {
	if !strings.HasPrefix(firstLine, "$") {
		return firstLine, nil
	}

	// Parse length
	lengthStr := strings.TrimPrefix(firstLine, "$")
	if lengthStr == "-1" {
		return "(nil)", nil
	}

	// Read actual content
	content, err := c.reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(content), nil
}

func (c *Client) ReadArray(firstLine string) ([]string, error) {
	if !strings.HasPrefix(firstLine, "*") {
		return []string{firstLine}, nil
	}

	// Parse array size
	sizeStr := strings.TrimPrefix(firstLine, "*")
	if sizeStr == "0" {
		return []string{}, nil
	}

	var size int
	fmt.Sscanf(sizeStr, "%d", &size)

	results := make([]string, 0, size)
	for i := 0; i < size; i++ {
		line, err := c.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)

		value, err := c.ReadBulkString(line)
		if err != nil {
			return nil, err
		}
		results = append(results, value)
	}

	return results, nil
}

func (c *Client) FormatResponse(response string) string {
	if len(response) == 0 {
		return ""
	}

	switch response[0] {
	case '+': // Simple string
		return strings.TrimPrefix(response, "+")
	case '-': // Error
		return fmt.Sprintf("(error) %s", strings.TrimPrefix(response, "-"))
	case ':': // Integer
		return strings.TrimPrefix(response, ":")
	case '$': // Bulk string
		value, _ := c.ReadBulkString(response)
		return value
	case '*': // Array
		values, _ := c.ReadArray(response)
		if len(values) == 0 {
			return "(empty array)"
		}
		result := ""
		for i, v := range values {
			result += fmt.Sprintf("%d) %s\n", i+1, v)
		}
		return strings.TrimRight(result, "\n")
	default:
		return response
	}
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func main() {
	addr := flag.String("h", "localhost:8080", "Server address (host:port)")
	flag.Parse()

	client, err := NewClient(*addr)
	if err != nil {
		fmt.Printf("Could not connect to BitCask at %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer client.Close()

	fmt.Printf("Connected to BitCask at %s\n", *addr)
	fmt.Println("Type 'help' for available commands, 'quit' to exit")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("\nbitcask> ")

		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		if strings.ToLower(input) == "help" {
			printHelp()
			continue
		}

		if strings.ToLower(input) == "quit" || strings.ToLower(input) == "exit" {
			fmt.Println("Goodbye!")
			break
		}

		start := time.Now()
		response, err := client.SendCommand(input)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		formatted := client.FormatResponse(response)
		fmt.Println(formatted)
		fmt.Printf("(%.2fms)\n", float64(elapsed.Microseconds())/1000.0)
	}
}

func printHelp() {
	help := `
Available Commands:
  SET key value       Set a key to hold a string value
  GET key            Get the value of a key
  DEL key            Delete a key
  EXISTS key         Check if a key exists (returns 1 or 0)
  KEYS pattern       Get all keys (pattern not implemented yet)
  DBSIZE             Return the number of keys
  SYNC               Force sync to disk
  PING               Ping the server
  INFO               Get server information
  QUIT               Close the connection

Examples:
  SET user:1 alice
  GET user:1
  SET msg "hello world"
  DEL user:1
  EXISTS user:1
  DBSIZE
  KEYS *
`
	fmt.Println(help)
}
