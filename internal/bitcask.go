package internal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type BitCask struct {
	// KeyDir is a in-memory hash table used to store all the keys referenced to the actual value
	KeyDir        map[string]ValuePointer
	Files         map[int]*os.File // Multiple file descriptors for read
	Mu            *sync.RWMutex
	CurrentFileId int
	ActiveFile    *os.File // ONLY 1 active file to write and it's always written at the end
	ActiveSize    int64    // Used to check whether this active file exceeds out of maximum allowed size, else trigger rollNewFile()
	dir           string
	// TESTING
	writer *bufio.Writer
	done   chan struct{}
	syncWg *sync.WaitGroup
}

type ValuePointer struct {
	FileId int
	Offset int64
	Size   int64
}

func Open(dir string) (*BitCask, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	bc := &BitCask{
		dir:    dir,
		KeyDir: make(map[string]ValuePointer),
		Files:  make(map[int]*os.File),
		done:   make(chan struct{}),
		syncWg: &sync.WaitGroup{},
		Mu:     &sync.RWMutex{},
	}

	if err := bc.LoadFiles(); err != nil {
		return nil, err
	}

	if bc.ActiveFile == nil {
		log.Println("ActiveFile is nil, rolling a new file")
		if err := bc.RollNewFile(); err != nil {
			return nil, fmt.Errorf("failed to roll new file: %v", err)
		}
	}

	// Initialize buffered writer
	bc.writer = bufio.NewWriterSize(bc.ActiveFile, 64*1024) // 64KB buffer

	// Start background sync
	bc.startBackgroundSync()

	return bc, nil
}

func (bc *BitCask) startBackgroundSync() {
	bc.syncWg.Add(1)

	go func() {
		defer bc.syncWg.Done()

		ticker := time.NewTicker(syncInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				bc.Mu.Lock()
				if bc.writer != nil {
					_ = bc.writer.Flush()
				}
				if bc.ActiveFile != nil {
					_ = bc.ActiveFile.Sync()
				}
				bc.Mu.Unlock()

			case <-bc.done:
				bc.Mu.Lock()
				if bc.writer != nil {
					_ = bc.writer.Flush()
				}
				if bc.ActiveFile != nil {
					_ = bc.ActiveFile.Sync()
				}
				bc.Mu.Unlock()
				return
			}
		}
	}()
}
func (bc *BitCask) Put(key string, value string) error {
	bc.Mu.Lock()
	defer bc.Mu.Unlock()
	entry := NewLogEntry(key, value, false)

	if bc.ActiveFile == nil || bc.ActiveSize+entry.Size() >= MaxActiveFileSize {
		if err := bc.RollNewFile(); err != nil {
			return fmt.Errorf("failed to roll new file: %w", err)
		}
	}

	offset := bc.ActiveSize

	n, err := writeLogEntryBuffered(bc.writer, entry)
	if err != nil {
		return fmt.Errorf("failed to write log entry: %w", err)
	}

	if err := bc.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}
	
	bc.KeyDir[key] = ValuePointer{
		FileId: bc.CurrentFileId,
		Offset: offset,
		Size:   entry.Size(),
	}
	bc.ActiveSize += int64(n)

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

	entry, err := readLogEntry(file, vp.Offset, vp.Size)
	if err != nil {
		return "", err
	}

	if entry.IsDeleted() {
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

	if bc.ActiveFile == nil || bc.ActiveSize+entry.Size() >= MaxActiveFileSize {
		if err := bc.RollNewFile(); err != nil {
			return fmt.Errorf("failed to roll new file: %w", err)
		}
	}

	n, err := writeLogEntryBuffered(bc.writer, entry)
	if err != nil {
		return fmt.Errorf("failed to write log entry: %w", err)
	}
	bc.ActiveSize += int64(n)
	delete(bc.KeyDir, key)

	return nil
}

func (bc *BitCask) RollNewFile() error {
	if bc.ActiveFile != nil {
		oldFileId := bc.CurrentFileId

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

	newId := bc.CurrentFileId + 1

	fileName := fmt.Sprintf("%06d.log", newId)
	filePath := filepath.Join(bc.dir, fileName)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	// Bitcask instance have a new active file and new currentFileId
	bc.CurrentFileId = newId
	bc.ActiveFile = file
	bc.ActiveSize = 0
	bc.Files[newId] = file

	bc.writer = bufio.NewWriterSize(file, 64*1024)

	return nil
}

func (bc *BitCask) LoadFiles() error {
	// recover() from the existing files from ./logs folder
	files, _ := filepath.Glob(filepath.Join(bc.dir, "*.log"))
	log.Println("BitCask data dir:", bc.dir)
	log.Println("Found log files:", files)

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

	bc.CurrentFileId = maxId

	if maxId > 0 {
		// The most recent file must be writable (active file). We initially opened
		// every file as read-only to rebuild KeyDir safely. Now reopen the latest
		// file with RW|APPEND so subsequent writes succeed.
		if f, ok := bc.Files[maxId]; ok && f != nil {
			_ = f.Close()
		}

		activePath := filepath.Join(bc.dir, fmt.Sprintf("%06d.log", maxId))
		activeFile, err := os.OpenFile(activePath, os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("failed to reopen active file for write: %w", err)
		}

		bc.Files[maxId] = activeFile
		bc.ActiveFile = activeFile

		offset, err := bc.ActiveFile.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("failed to seek active file: %w", err)
		}
		bc.ActiveSize = offset

		bc.writer = bufio.NewWriterSize(bc.ActiveFile, 64*1024)
	}

	return nil
}

func (bc *BitCask) rebuildKeyDirFromFile(file *os.File, fileId int) error {
	var offset int64 = 0

	for {
		entry, size, err := parseEntry(file)
		if err != nil {
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}

		if entry.IsDeleted() {
			// Remove deleted keys
			delete(bc.KeyDir, string(entry.Key))
		} else {
			// Update KeyDir with latest value location
			bc.KeyDir[string(entry.Key)] = ValuePointer{
				FileId: fileId,
				Offset: offset,
				Size:   size,
			}
		}

		offset += size
	}

	return nil
}

func (bc *BitCask) Sync() error {
	bc.Mu.Lock()
	defer bc.Mu.Unlock()

	if bc.writer != nil {
		if err := bc.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush buffer: %w", err)
		}
	}

	if bc.ActiveFile != nil {
		if err := bc.ActiveFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync to disk: %w", err)
		}
	}

	return nil
}

func (bc *BitCask) Close() error {
	close(bc.done)
	bc.syncWg.Wait()
	bc.Mu.Lock()
	defer bc.Mu.Unlock()

	if bc.writer != nil {
		if err := bc.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush buffer on close: %w", err)
		}
	}

	if bc.ActiveFile != nil {
		if err := bc.ActiveFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync on close: %w", err)
		}
	}

	for id, file := range bc.Files {
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close file %d: %w", id, err)
		}
	}

	return nil
}
