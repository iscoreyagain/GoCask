package internal

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type BitCask struct {
	// KeyDir is a in-memory hash table used to store all the keys referenced to the actual value
	KeyDir        map[string]ValuePointer
	Files         map[int]*os.File // Multiple file descriptors for read
	Mu            *sync.RWMutex
	currentFileId int
	ActiveFile    *os.File // ONLY 1 active file to write and it's always written at the end
	activeSize    int64    // Used to check whether this active file exceeds out of maximum allowed size, else trigger rollNewFile()
	dir           string
}

type ValuePointer struct {
	FileId int
	Offset int64
	Size   int64
}

func (bc *BitCask) Put(key string, value string) error {
	bc.Mu.Lock()
	defer bc.Mu.Unlock()
	entry := NewLogEntry(key, value, false)

	if bc.ActiveFile == nil || bc.activeSize+entry.Size() >= MaxActiveFileSize {
		if err := bc.rollNewFile(); err != nil {
			return fmt.Errorf("failed to roll new file: %w", err)
		}
	}

	offset := bc.activeSize

	n, err := writeLogEntry(bc.ActiveFile, entry)
	if err != nil {
		return fmt.Errorf("failed to write log entry: %w", err)
	}

	bc.KeyDir[key] = ValuePointer{
		FileId: bc.currentFileId,
		Offset: offset,
		Size:   int64(entry.valueSize),
	}

	bc.activeSize += int64(n)

	return nil
}

func (bc *BitCask) Get(key string) (string, error) {
	bc.Mu.RLock()
	defer bc.Mu.RUnlock()

	vp, ok := bc.KeyDir[key]
	if !ok {
		return "", fmt.Errorf("key not found!")
	}

	file, ok := bc.Files[vp.FileId]
	if !ok {
		return "", fmt.Errorf("file not found!")
	}

	entry, err := readLogEntry(file, vp.Offset)
	if err != nil {
		return "", err
	}

	if entry.tombstone {
		return "", fmt.Errorf("key not found")
	}

	return string(entry.Value), nil
}

func (bc *BitCask) Delete(key string) error {
	bc.Mu.Lock()
	defer bc.Mu.Unlock()

	if _, ok := bc.KeyDir[key]; !ok {
		return fmt.Errorf("key not found")
	}

	entry := NewLogEntry(key, "", true)

	if bc.ActiveFile == nil || bc.activeSize+entry.Size() >= MaxActiveFileSize {
		if err := bc.rollNewFile(); err != nil {
			return fmt.Errorf("failed to roll new file: %w", err)
		}
	}

	n, err := writeLogEntry(bc.ActiveFile, entry)
	if err != nil {
		return fmt.Errorf("failed to write log entry: %w", err)
	}
	bc.activeSize += int64(n)
	delete(bc.KeyDir, key)

	return nil
}

func Init() *BitCask {
	baseDir := "./logs"
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		log.Fatal(err)
	}

	bc := &BitCask{
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

func (bc *BitCask) rollNewFile() error {
	if bc.ActiveFile != nil {
		oldFileId := bc.currentFileId

		// Move old write-only file into a map of read-only files
		if err := bc.ActiveFile.Sync(); err != nil {
			return err
		}
		if err := bc.ActiveFile.Close(); err != nil {
			return err
		}

		oldPath := filepath.Join(bc.dir, fmt.Sprintf("%06d.log", oldFileId))
		readFile, err := os.OpenFile(oldPath, os.O_RDONLY, 0644)
		if err != nil {
			return err
		}
		bc.Files[oldFileId] = readFile
	}

	newId := bc.currentFileId + 1

	fileName := fmt.Sprintf("%06d.log", newId)
	filePath := filepath.Join(bc.dir, fileName)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	// Bitcask instance have a new active file and new currentFileId
	bc.currentFileId = newId
	bc.ActiveFile = file
	bc.activeSize = 0

	bc.Files[newId] = file

	return nil
}

func (bc *BitCask) loadFiles() error { // recover() from the existing files from ./logs folder
	files, _ := filepath.Glob(filepath.Join(bc.dir, "*.log"))

	bc.Files = make(map[int]*os.File)
	maxId := 0

	for _, file := range files {
		base := filepath.Base(file) // "000001.log"

		name := strings.TrimSuffix(base, ".log")

		id, err := strconv.Atoi(name)
		if err != nil {
			continue
		}

		if id > maxId {
			maxId = id
		}

		f, err := os.OpenFile(file, os.O_RDONLY, 0644)
		if err != nil {
			return err
		}

		bc.Files[id] = f

		if err := bc.rebuildKeyDirFromFile(f, id); err != nil {
			return fmt.Errorf("failed to rebuild keydir from %s: %w", file, err)
		}
	}

	bc.currentFileId = maxId

	if maxId > 0 {
		bc.ActiveFile = bc.Files[maxId]
		offset, _ := bc.ActiveFile.Seek(0, io.SeekEnd)
		bc.activeSize = offset
	}

	return nil
}

func (bc *BitCask) rebuildKeyDirFromFile(file *os.File, fileId int) error {
	var offset int64 = 0

	for {
		entry, err := readLogEntry(file, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if entry.tombstone {
			// Remove deleted keys
			delete(bc.KeyDir, string(entry.Key))
		} else {
			// Update KeyDir with latest value location
			bc.KeyDir[string(entry.Key)] = ValuePointer{
				FileId: fileId,
				Offset: offset,
				Size:   int64(entry.valueSize),
			}
		}

		offset += entry.Size()
	}

	return nil
}
