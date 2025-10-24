package internal

import (
	"log"
	"os"
	"sync"
)

var bc *BitCask

func Init() *BitCask {
	baseDir := "./logs"
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		log.Fatal(err)
	}

	bc = &BitCask{
		KeyDir:        make(map[string]ValuePointer),
		Files:         make(map[int]*os.File),
		Mu:            &sync.RWMutex{},
		currentFileId: 0,
		ActiveFile:    nil,
		activeSize:    0,
		dir:           baseDir,
	}
	if err := bc.loadFiles(); err != nil {
		log.Printf("Failed to load existing log files: %v\n", err)
	}

	if bc.ActiveFile == nil {
		if err := bc.rollNewFile(); err != nil {
			log.Fatalf("Failed to create new active file: %v\n", err)
		}
	}

	return bc
}

func InitWithDir(baseDir string) *BitCask {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		log.Fatal(err)
	}

	bc = &BitCask{
		KeyDir:        make(map[string]ValuePointer),
		Files:         make(map[int]*os.File),
		Mu:            &sync.RWMutex{},
		currentFileId: 0,
		ActiveFile:    nil,
		activeSize:    0,
		dir:           baseDir,
	}

	return bc
}
