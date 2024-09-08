package strukture

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	// Ovo ce pomoci pri deserijalizaciji Entry-ja.

	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

type Entry struct {
	/*
		+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
		|    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
		+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+

		CRC = 32bit hash computed over the payload using CRC
		Key Size = Length of the Key data
		Tombstone = If this record was deleted and has a value
		Value Size = Length of the Value data
		Key = Key data
		Value = Value data
		Timestamp = Timestamp of the operation in seconds
	*/

	CRC       uint32
	timestamp time.Time
	tombstone uint8
	keySize   uint64
	valueSize uint64
	key       string
	value     []byte

	// Dokumentacija trazi timestamp od 16B. Ovdje je pretvoren u 8B jer nisam siguran da ijedna konverzija
	// time.Time vrijednosti podrzava velicinu od 16B. 8B implementacija bi trebala biti sasvim dovoljna jer
	// podrzava mjerenje vremena do preciznosti jedne mikrosekunde u narednih ~300`000 godina.
	// Mogli bismo ovo pretvoriti u 16B podatak tako sto bismo dodali 64 nule ispred ovoga, ali zasto?
}

func (e *Entry) GetValue() []byte {
	return e.value
}

func CreateEntry(key string, data []byte, tombstone uint8) *Entry {
	return &Entry{crc32.ChecksumIEEE(data), time.Now(), tombstone, uint64(len([]byte(key))), uint64(len(data)), key, data}
}

func (entry Entry) ToByteArray() []byte {
	byteArray := make([]byte, 0)

	byteArray = binary.BigEndian.AppendUint32(byteArray, entry.CRC)
	byteArray = binary.BigEndian.AppendUint64(byteArray, uint64(time.Time.UnixMicro(entry.timestamp)))
	byteArray = append(byteArray, entry.tombstone)
	byteArray = binary.BigEndian.AppendUint64(byteArray, entry.keySize)
	byteArray = binary.BigEndian.AppendUint64(byteArray, entry.valueSize)
	byteArray = append(byteArray, []byte(entry.key)...)
	byteArray = append(byteArray, []byte(entry.value)...)

	return byteArray
}

func (entry *Entry) Size() int64 {
	return int64(29 + entry.keySize + entry.valueSize)
}

func EntryTest() {
	e1 := CreateEntry("1", []byte{0, 1, 2, 3, 4}, 0)
	e2 := CreateEntry("02", []byte("abcde"), 1)
	e3 := CreateEntry("003", []byte("Test string"), 0)

	fmt.Print(len(e1.ToByteArray()), " ", e1.ToByteArray(), "\n")
	fmt.Print(len(e2.ToByteArray()), " ", e2.ToByteArray(), "\n")
	fmt.Print(len(e3.ToByteArray()), " ", e3.ToByteArray(), "\n")
}

// NIJE MOJE ---------------------------------------------------------------------------------------------------------------------------------------------------
// MOZDA MOZE POMOCI AKO ZATREBA KASNIJE
/*
func SerializeEntry(entry WriteAheadLogEntry) ([]byte, int) {
	// returns the serialized entry and the size of it
	// first we create all of the parts of the serialized entry, and join them in the end
	crc := make([]byte, CRC_SIZE)
	timestamp := make([]byte, TIMESTAMP_SIZE)
	tombstone := make([]byte, TOMBSTONE_SIZE)
	keysize := make([]byte, KEY_SIZE_SIZE)
	valuesize := make([]byte, VALUE_SIZE_SIZE)

	binary.BigEndian.PutUint64(timestamp, uint64(entry.Timestamp.Unix()))
	if entry.Tombstone {
		tombstone[0] = 1
	} else {
		tombstone[0] = 0
	}

	binary.BigEndian.PutUint64(keysize, uint64(len(entry.Key)))
	binary.BigEndian.PutUint64(valuesize, uint64(len(entry.Value)))

	returnArray := append(timestamp, tombstone...)
	returnArray = append(returnArray, keysize...)
	returnArray = append(returnArray, valuesize...)
	returnArray = append(returnArray, entry.Key...)
	returnArray = append(returnArray, entry.Value...)

	crc = hashfunc.Crc32AsBytes(returnArray)

	returnArray = append(crc, returnArray...)

	return returnArray, len(returnArray)
}

func deserializeEntry(data []byte) (*WriteAheadLogEntry, []byte, error) {
	reader := bytes.NewReader(data)

	crc := make([]byte, CRC_SIZE)
	timestampBytes := make([]byte, TIMESTAMP_SIZE)
	tombstone := make([]byte, TOMBSTONE_SIZE)
	keysize := make([]byte, KEY_SIZE_SIZE)
	valuesize := make([]byte, VALUE_SIZE_SIZE)

	err := binary.Read(reader, binary.BigEndian, &crc)
	if err != nil {
		return nil, nil, errors.New("error while reading crc from a wal entry")
	}
	err = binary.Read(reader, binary.BigEndian, &timestampBytes)
	if err != nil {
		return nil, nil, errors.New("error while reading timestamp from a wal entry")
	}
	if _, err = reader.Read(tombstone); err != nil {
		return nil, nil, err
	}
	err = binary.Read(reader, binary.BigEndian, &keysize)
	if err != nil {
		return nil, nil, errors.New("error while reading keysize from a wal entry")
	}
	err = binary.Read(reader, binary.BigEndian, &valuesize)
	if err != nil {
		return nil, nil, errors.New("error while reading valuesize from a wal entry")
	}

	timestamp := time.Unix(int64(binary.BigEndian.Uint64(timestampBytes)), 0)

	key := make([]byte, binary.BigEndian.Uint64(keysize))
	value := make([]byte, binary.BigEndian.Uint64(valuesize))

	if _, err := reader.Read(key); err != nil {
		return nil, nil, err
	}

	if _, err := reader.Read(value); err != nil {
		return nil, nil, err
	}

	entry := &WriteAheadLogEntry{
		Timestamp: timestamp,
		Tombstone: tombstone[0] == 1,
		Key:       key,
		Value:     value,
	}

	return entry, crc, nil
}
*/
