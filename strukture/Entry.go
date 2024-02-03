package strukture

import (
	hashfunc "NASP_projekat2023/utils"
	"encoding/binary"
	"errors"
	"time"
)

type MemtableEntry struct {
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Tombstone bool
}

func NewMemtableEntry(key, value []byte, tombstone bool, timestamp time.Time) *MemtableEntry {
	return &MemtableEntry{key, value, timestamp, tombstone}
}

func EmptyMemtableEntry(key []byte, timestamp time.Time) *MemtableEntry {
	return &MemtableEntry{
		Key:       key,
		Value:     nil,
		Timestamp: timestamp,
		Tombstone: true,
	}
}

func SerializeMemtableEntry(entry MemtableEntry) []byte {
	buf := make([]byte, 0, 1024)
	var b [binary.MaxVarintLen64]byte

	n := binary.PutUvarint(b[:], uint64(entry.Timestamp.Unix()))
	buf = append(buf, b[:n]...)

	if entry.Tombstone {
		buf = append(buf, 't')
	} else {
		buf = append(buf, 'f')
	}

	n = binary.PutUvarint(b[:], uint64(len(entry.Key)))
	buf = append(buf, b[:n]...)

	if !entry.Tombstone {
		n = binary.PutUvarint(b[:], uint64(len(entry.Value)))
		buf = append(buf, b[:n]...)
	}

	buf = append(buf, entry.Key...)

	if !entry.Tombstone {
		buf = append(buf, entry.Value...)
	}

	crc := hashfunc.Crc32(buf)
	n = binary.PutUvarint(b[:], uint64(crc))
	buf = append(b[:n], buf...)

	return buf
}

func DeserializeMemtableEntry(buf []byte) (MemtableEntry, int, error) {
	var decodedEntry MemtableEntry
	initialLen := len(buf)

	if len(buf) < 4 {
		return decodedEntry, 0, errors.New("buffer too short for CRC")
	}

	_, n := binary.Uvarint(buf)
	buf = buf[n:]

	timestamp, n := binary.Uvarint(buf)
	if n <= 0 {
		return decodedEntry, 0, errors.New("buffer too short for timestamp")
	}
	decodedEntry.Timestamp = time.Unix(int64(timestamp), 0)
	buf = buf[n:]

	if len(buf) < 1 {
		return decodedEntry, 0, errors.New("buffer too short for tombstone")
	}

	decodedEntry.Tombstone = buf[0] == 't'
	buf = buf[1:]

	keyLen, n := binary.Uvarint(buf)
	if n <= 0 || len(buf[n:]) < int(keyLen) {
		return decodedEntry, 0, errors.New("buffer too short for key")
	}
	buf = buf[n:]

	var valueLen uint64
	if !decodedEntry.Tombstone {
		valueLen, n = binary.Uvarint(buf)
		if n <= 0 || len(buf[n:]) < int(valueLen) {
			return decodedEntry, 0, errors.New("buffer too short for value size")
		}
		buf = buf[n:]
	}

	decodedEntry.Key = buf[:keyLen]
	buf = buf[keyLen:]

	if !decodedEntry.Tombstone {
		decodedEntry.Value = buf[:valueLen]
		buf = buf[valueLen:]
	}

	bytesRead := initialLen - len(buf)
	return decodedEntry, bytesRead, nil
}
