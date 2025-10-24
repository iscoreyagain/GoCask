package internal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"time"
)

type LogEntry struct {
	crc       uint32
	timestamp int64
	keySize   uint32
	valueSize uint32
	tombstone bool
	Key       []byte
	Value     []byte
}

// logEntryHeaderSize is the size in bytes of the fixed-size header
// preceding the variable-length key and value within a log entry.
const logEntryHeaderSize int64 = 4 + 8 + 4 + 4 + 1

// Write the decoded entry into the append-only write file and return the size of entry (err if it occurs)
func writeLogEntry(file *os.File, entry *LogEntry) (int, error) {
	total := 0

	fields := []interface{}{
		entry.crc,
		entry.timestamp,
		entry.keySize,
		entry.valueSize,
		entry.tombstone,
	}

	for _, field := range fields {
		if err := binary.Write(file, binary.BigEndian, field); err != nil {
			return total, err
		}

		switch field.(type) {
		case uint32:
			total += 4
		case int64:
			total += 8
		case bool:
			total += 1
		}
	}

	n, err := file.Write(entry.Key)
	if err != nil {
		return 0, err
	}
	total += n

	n, err = file.Write(entry.Value)
	if err != nil {
		return 0, err
	}
	total += n

	return total, nil
}

func readLogEntry(file *os.File, offset int64) (*LogEntry, error) {
    entry := new(LogEntry)

    // Use pread-style reads that do not mutate the file offset.
    // Read the fixed-size header first.
    headerReader := io.NewSectionReader(file, offset, logEntryHeaderSize)

    if err := binary.Read(headerReader, binary.BigEndian, &entry.crc); err != nil {
        return nil, err
    }
    if err := binary.Read(headerReader, binary.BigEndian, &entry.timestamp); err != nil {
        return nil, err
    }
    if err := binary.Read(headerReader, binary.BigEndian, &entry.keySize); err != nil {
        return nil, err
    }
    if err := binary.Read(headerReader, binary.BigEndian, &entry.valueSize); err != nil {
        return nil, err
    }
    if err := binary.Read(headerReader, binary.BigEndian, &entry.tombstone); err != nil {
        return nil, err
    }

    // Read key and value using ReadAt via SectionReader to ensure full reads.
    keyLen := int64(entry.keySize)
    valLen := int64(entry.valueSize)

    entry.Key = make([]byte, keyLen)
    if _, err := io.ReadFull(io.NewSectionReader(file, offset+logEntryHeaderSize, keyLen), entry.Key); err != nil {
        return nil, err
    }

    valueOffset := offset + logEntryHeaderSize + keyLen
    entry.Value = make([]byte, valLen)
    if _, err := io.ReadFull(io.NewSectionReader(file, valueOffset, valLen), entry.Value); err != nil {
        return nil, err
    }

    return entry, nil
}

// readLogEntryWithSize reads and decodes a log entry at the given offset using
// the known total size of the entry. This avoids re-deriving the total length
// from the header fields and enables a single contiguous read from disk.
func readLogEntryWithSize(file *os.File, offset int64, size int64) (*LogEntry, error) {
    if size < logEntryHeaderSize {
        return nil, io.ErrUnexpectedEOF
    }

    buf := make([]byte, size)
    if _, err := io.ReadFull(io.NewSectionReader(file, offset, size), buf); err != nil {
        return nil, err
    }

    r := bytes.NewReader(buf)
    entry := new(LogEntry)

    if err := binary.Read(r, binary.BigEndian, &entry.crc); err != nil {
        return nil, err
    }
    if err := binary.Read(r, binary.BigEndian, &entry.timestamp); err != nil {
        return nil, err
    }
    if err := binary.Read(r, binary.BigEndian, &entry.keySize); err != nil {
        return nil, err
    }
    if err := binary.Read(r, binary.BigEndian, &entry.valueSize); err != nil {
        return nil, err
    }
    if err := binary.Read(r, binary.BigEndian, &entry.tombstone); err != nil {
        return nil, err
    }

    keyLen := int(entry.keySize)
    valLen := int(entry.valueSize)

    // Sanity-check that the provided size matches header+payload
    expected := int(logEntryHeaderSize) + keyLen + valLen
    if len(buf) < expected {
        return nil, io.ErrUnexpectedEOF
    }

    entry.Key = make([]byte, keyLen)
    if _, err := io.ReadFull(r, entry.Key); err != nil {
        return nil, err
    }
    entry.Value = make([]byte, valLen)
    if _, err := io.ReadFull(r, entry.Value); err != nil {
        return nil, err
    }

    return entry, nil
}

// readLogEntryHeaderAndKey reads only the fixed-size header and the key bytes
// at the given offset, returning the tombstone flag, key, and total entry size
// computed from the header fields. It does not read or allocate the value.
func readLogEntryHeaderAndKey(file *os.File, offset int64) (bool, []byte, int64, error) {
    // Read header fully using ReadAt semantics.
    headerBuf := make([]byte, logEntryHeaderSize)
    n, err := file.ReadAt(headerBuf, offset)
    if err != nil {
        if err == io.EOF && int64(n) < logEntryHeaderSize {
            return false, nil, 0, io.EOF
        }
        if err != nil && err != io.EOF {
            return false, nil, 0, err
        }
    }
    if int64(n) < logEntryHeaderSize {
        return false, nil, 0, io.EOF
    }

    r := bytes.NewReader(headerBuf)
    var (
        crc       uint32
        ts        int64
        keySize   uint32
        valueSize uint32
        tombstone bool
    )
    if err := binary.Read(r, binary.BigEndian, &crc); err != nil {
        return false, nil, 0, err
    }
    if err := binary.Read(r, binary.BigEndian, &ts); err != nil {
        return false, nil, 0, err
    }
    if err := binary.Read(r, binary.BigEndian, &keySize); err != nil {
        return false, nil, 0, err
    }
    if err := binary.Read(r, binary.BigEndian, &valueSize); err != nil {
        return false, nil, 0, err
    }
    if err := binary.Read(r, binary.BigEndian, &tombstone); err != nil {
        return false, nil, 0, err
    }

    totalSize := logEntryHeaderSize + int64(keySize) + int64(valueSize)

    // Read only the key
    key := make([]byte, int(keySize))
    if len(key) > 0 {
        kn, kerr := file.ReadAt(key, offset+logEntryHeaderSize)
        if kerr != nil {
            return false, nil, 0, kerr
        }
        if kn != len(key) {
            return false, nil, 0, io.ErrUnexpectedEOF
        }
    }

    return tombstone, key, totalSize, nil
}

func NewLogEntry(key string, value string, tombstone bool) *LogEntry {
	timestamp := time.Now().UnixNano()
	keySize := uint32(len([]byte(key)))
	valueSize := uint32(len([]byte(value)))

	// data byte slice to calculate CRC
	data := new(bytes.Buffer)
	binary.Write(data, binary.BigEndian, timestamp)
	binary.Write(data, binary.BigEndian, keySize)
	binary.Write(data, binary.BigEndian, valueSize)
	binary.Write(data, binary.BigEndian, tombstone)
	data.Write([]byte(key))
	data.Write([]byte(value))
	crc := calcCRC(data.Bytes())

	return &LogEntry{
		crc:       crc,
		timestamp: timestamp,
		keySize:   keySize,
		valueSize: valueSize,
		tombstone: tombstone,
		Key:       []byte(key),
		Value:     []byte(value),
	}
}

func calcCRC(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
}

// Return the total size of the entry
func (e *LogEntry) Size() int64 {
	return int64(4 + 8 + 4 + 4 + 1 + len(e.Key) + len(e.Value))
}
