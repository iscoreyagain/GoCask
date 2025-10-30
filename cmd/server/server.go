package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/iscoreyagain/GoCask/internal"
	"github.com/iscoreyagain/GoCask/internal/config"
	"github.com/iscoreyagain/GoCask/internal/core"
)

type Server struct {
	bc       *internal.BitCask
	listener net.Listener
	address  string
}

func NewServer(dataDir string) (*Server, error) {
	bc, err := internal.Open(dataDir)
	if err != nil {
		return nil, fmt.Errorf("Failed to open bitcask: %v", err)
	}

	if bc == nil {
		return nil, fmt.Errorf("BitCask returned nil")
	}

	log.Printf("BitCask initialized with %d keys", len(bc.KeyDir))

	return &Server{
		bc:      bc,
		address: config.Address,
	}, nil
}

func (s *Server) Start() error {
	listener, err := net.Listen(config.Protocol, s.address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.listener = listener
	log.Printf("BitCask server listening on %s", s.address)
	log.Printf("Ready to accept connections")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in handleConnection: %v", r)
			conn.Close()
		}
	}()

	defer conn.Close()

	if s == nil || s.bc == nil {
		log.Printf("Server or BitCask is nil, closing connection")
		return
	}

	clientAddr := conn.RemoteAddr().String()
	log.Printf("New client connected: %s", clientAddr)

	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	for scanner.Scan() {
		line := scanner.Text()
		cmd, err := core.ParseCommand(line)
		if err != nil {
			log.Printf("Error parsing command: %v", err)
		}
		response := core.ExecuteAndResponse(cmd)

		writer.WriteString(response + "\n")
		writer.Flush()
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Client %s error: %v", clientAddr, err)
	}

	log.Printf("Client disconnected: %s", clientAddr)
}

func (s *Server) Close() error {
	if s.listener != nil {
		s.listener.Close()
	}
	if s.bc != nil {
		return s.bc.Close()
	}
	return nil
}

func main() {
	dataDir := flag.String("data", "./data", "Data directory")
	flag.Parse()

	server, err := NewServer(*dataDir)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	core.SetBitCask(server.bc)

	defer server.Close()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("\nShutting down gracefully...")
		server.Close()
		os.Exit(0)
	}()

	if err := server.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
