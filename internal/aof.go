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
    const headerSize int64 = 4 + 8 + 4 + 4 + 1
    headerReader := io.NewSectionReader(file, offset, headerSize)

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
    if _, err := io.ReadFull(io.NewSectionReader(file, offset+headerSize, keyLen), entry.Key); err != nil {
        return nil, err
    }

    valueOffset := offset + headerSize + keyLen
    entry.Value = make([]byte, valLen)
    if _, err := io.ReadFull(io.NewSectionReader(file, valueOffset, valLen), entry.Value); err != nil {
        return nil, err
    }

    return entry, nil
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
