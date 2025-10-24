package internal

import (
	"errors"
	"fmt"
	"io"
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
		Size:   entry.Size(),
	}

	bc.activeSize += int64(n)

	if err := bc.ActiveFile.Sync(); err != nil {
		return err
	}

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
		bc.activeSize = offset
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
