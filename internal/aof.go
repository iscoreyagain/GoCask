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
	Header *Header
	Key    []byte
	Value  []byte
}

type Header struct {
	Crc       uint32
	Timestamp int64
	KeySize   uint32
	ValueSize uint32
	Tombstone bool
}

// Write the decoded entry into the append-only write file and return the size of entry (err if it occurs)
func writeLogEntry(file *os.File, entry *LogEntry) (int, error) {
	total := 0

	if err := binary.Write(file, binary.BigEndian, entry.Header); err != nil {
		return total, err
	}
	total += logEntryHeaderSize

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

func readLogEntry(file *os.File, offset int64, size int64) (*LogEntry, error) {
	if size < logEntryHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}

	buf := make([]byte, size)
	n, err := file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if int64(n) != size {
		return nil, io.ErrUnexpectedEOF
	}

	r := bytes.NewReader(buf)

	header := new(Header)
	entry := new(LogEntry)
	entry.Header = header

	if err := binary.Read(r, binary.BigEndian, header); err != nil {
		return nil, err
	}

	keyLen, valLen := int(header.KeySize), int(header.ValueSize)
	if logEntryHeaderSize+keyLen+valLen != len(buf) {
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

// This function will parse each entry in .log files and append it into KeyDir for lightning read.
func parseEntry(file *os.File) (*LogEntry, int64, error) {
	entry := new(LogEntry)
	entry.Header = new(Header)

	if err := binary.Read(file, binary.BigEndian, entry.Header); err != nil {
		return nil, 0, io.ErrUnexpectedEOF
	}

	key := make([]byte, entry.Header.KeySize)
	if _, err := io.ReadFull(file, key); err != nil {
		return nil, 0, err
	}

	val := make([]byte, entry.Header.ValueSize)
	if _, err := io.ReadFull(file, val); err != nil {
		return nil, 0, err
	}

	entry.Key = key
	entry.Value = val

	totalSz := int64(logEntryHeaderSize + len(key) + len(val))

	return entry, totalSz, nil
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

	header := &Header{
		Crc:       crc,
		Timestamp: timestamp,
		KeySize:   keySize,
		ValueSize: valueSize,
		Tombstone: tombstone,
	}
	return &LogEntry{
		Header: header,
		Key:    []byte(key),
		Value:  []byte(value),
	}
}

func calcCRC(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
}

// Return the total size of the entry
func (e *LogEntry) Size() int64 {
	return int64(logEntryHeaderSize + e.Header.KeySize + e.Header.ValueSize)
}

func (e *LogEntry) IsDeleted() bool {
	return e.Header.Tombstone
}
