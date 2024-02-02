package strukture

import (
	"encoding/binary"
	"errors"
	"fmt"
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

type Memtable struct {
	size        int
	currentSize int

	dataType     string
	dataHashMap  map[string]MemtableEntry
	dataSkipList *SkipList
	dataBTree    *BTree
}

func NewMemtable(MemTableSize, SkipListDepth, BTreeDegree int, MemTableType string) (*Memtable, error) {

	return &Memtable{
		MemTableSize,
		0,
		MemTableType,
		make(map[string]MemtableEntry),
		NewSkipList(SkipListDepth),
		NewBTree(BTreeDegree),
	}, nil
}

func (mt *Memtable) Insert(entry *MemtableEntry) error {

	if mt.dataType == "skip_list" {
		err := mt.InsertSkipList(entry)
		if err != nil {
			return err
		}

	} else if mt.dataType == "b_tree" {
		err := mt.InsertBTree(entry)
		if err != nil {
			return err
		}

	} else if mt.dataType == "hash_map" {
		err := mt.InsertHashMap(entry)
		if err != nil {
			return err
		}

	} else {
		return errors.New("Los naziv strukture kod Memtable.Insert().")
	}

	return nil
}

func (mt *Memtable) InsertSkipList(entry *MemtableEntry) error {
	exist := mt.dataSkipList.Search(entry.Key)

	if exist == nil {
		mt.dataSkipList.Insert(*entry)
		mt.currentSize += 1

		return nil
	}
	return errors.New("Same key already here lol")
}

func (mt *Memtable) InsertBTree(entry *MemtableEntry) error {
	err := mt.dataBTree.Insert(*entry)

	if err != nil {
		return err
	}

	mt.currentSize += 1
	return nil
}

func (mt *Memtable) InsertHashMap(entry *MemtableEntry) error {
	_, exist := mt.dataHashMap[string(entry.Key)]

	if exist {
		return errors.New("Same key already here lol")
	}

	mt.dataHashMap[string(entry.Key)] = *entry
	mt.currentSize += 1
	return nil
}

func (mt *Memtable) Delete(key []byte) error {
	if mt.dataType == "skip_list" {
		return mt.DeleteSkipList(key)
	}

	if mt.dataType == "b_tree" {
		return mt.DeleteBTree(key)
	}

	if mt.dataType == "hash_map" {
		return mt.DeleteHashMap(key)
	}

	return errors.New("Los naziv strukture kod Memtable.Delete().")
}

func (mt *Memtable) DeleteSkipList(key []byte) error {
	success := mt.dataSkipList.Delete(key)

	if success {
		mt.currentSize--
		return nil
	}
	return errors.New("error while deleting from a skiplist")
}

func (mt *Memtable) DeleteBTree(key []byte) error {
	success := mt.dataBTree.Delete(key)

	if success {
		mt.currentSize--
		return nil
	}
	return errors.New("error while deleting from a btree")
}

func (mt *Memtable) DeleteHashMap(key []byte) error {
	_, exist := mt.dataHashMap[string(key)]

	if exist {
		delete(mt.dataHashMap, string(key))
		mt.currentSize--
		return nil
	}
	return errors.New("error while deleting from a hashmap")
}

func (mt *Memtable) Exists(key []byte) bool {
	switch mt.dataType {
	case "skip_list":
		if mt.dataSkipList.Search(key) == nil {
			return false
		}
	case "b_tree":
		_, exists := mt.dataBTree.Search(key)
		if !exists {
			return false
		}
	case "hash_map":
		_, exists := mt.dataHashMap[string(key)]
		if !exists {
			return false
		}
	}
	return true
}

func (mt *Memtable) Get(key []byte) (*MemtableEntry, error) {
	if mt.dataType == "skip_list" {
		return mt.GetSkipList(key)
	}

	if mt.dataType == "b_tree" {
		return mt.GetBTree(key)
	}

	if mt.dataType == "hash_map" {
		return mt.GetHashMap(key)
	}

	return nil, errors.New("Los naziv strukture kod Memtable.Get().")
}

func (mt *Memtable) GetSkipList(key []byte) (*MemtableEntry, error) {
	entry := mt.dataSkipList.Search(key)

	if entry == nil {
		return nil, errors.New("Zapis ne postoji.")
	}
	return entry, nil
}

func (mt *Memtable) GetBTree(key []byte) (*MemtableEntry, error) {
	entry, exist := mt.dataBTree.Search(key)

	if !exist {
		return nil, errors.New("Zapis ne postoji.")
	}
	return entry, nil
}

func (mt *Memtable) GetHashMap(key []byte) (*MemtableEntry, error) {
	value, exist := mt.dataHashMap[string(key)]

	if !exist {
		return nil, errors.New("Zapis ne postoji.")
	}
	return &value, nil
}

func (mt *Memtable) IsFull() bool {
	if mt.currentSize == mt.size {
		return true
	}
	return false
}

func (mt *Memtable) PrintMemtable() {
	fmt.Print("\n", mt.size, " ", mt.currentSize, "\n")

	if mt.dataType == "hash_map" {
		for index, data := range mt.dataHashMap {
			fmt.Print("\n", index, ": ", data)
		}
	}
	if mt.dataType == "skip_list" {
		mt.dataSkipList.Print()
	}
	if mt.dataType == "b_tree" {
		mt.dataBTree.PrintTree(mt.dataBTree.root, 1)
	}
}

func SerializeMemtableEntry(entry MemtableEntry) []byte {
	buf := make([]byte, 0, 1024)
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], uint64(len(entry.Key)))
	buf = append(buf, b[:n]...)
	buf = append(buf, entry.Key...)
	n = binary.PutUvarint(b[:], uint64(len(entry.Value)))
	buf = append(buf, b[:n]...)
	buf = append(buf, entry.Value...)
	n = binary.PutUvarint(b[:], uint64(entry.Timestamp.Unix()))
	buf = append(buf, b[:n]...)
	buf = append(buf, []byte(fmt.Sprintf("%t", entry.Tombstone))...)
	return buf
}

func DeserializeMemtableEntry(buf []byte) MemtableEntry {
	var decodedEntry MemtableEntry
	keyLen, n := binary.Uvarint(buf)
	decodedEntry.Key = buf[n : n+int(keyLen)]
	buf = buf[n+int(keyLen):]
	valueLen, n := binary.Uvarint(buf)
	decodedEntry.Value = buf[n : n+int(valueLen)]
	buf = buf[n+int(valueLen):]
	timestamp, n := binary.Uvarint(buf)
	decodedEntry.Timestamp = time.Unix(int64(timestamp), 0)
	buf = buf[n:]
	decodedEntry.Tombstone = buf[0] == 't'
	return decodedEntry
}

// func (mt *Memtable) Flush(bloomfilter *BloomFilter, filename string) error {
func (mt *Memtable) Flush() error {

	/*
		// Poziva se kada treshold >= size. (Moze li biti vece ili mora striktno jednako?
		if mt.dataType == "hash_map" {
			keysToFlush := make([]string, 0)
			for key := range mt.dataHashMap {
				keysToFlush = append(keysToFlush, key)
			}
			sort.Strings(keysToFlush)

			for _, key := range keysToFlush {
				bloomfilter.Insert(key)

			}
		}
		if mt.dataType == "skip_list" {
			keysToFlush := make([]string, 0)
			node := mt.dataSkipList.head
			for node != nil {
				keysToFlush = append(keysToFlush, string(node.key))
				node = node.down
			}
			sort.Strings(keysToFlush)

			for _, key := range keysToFlush {
				bloomfilter.Insert(key)
			}
			return nil
		}
		if mt.dataType == "b_tree" {
			// pairs := mt.dataBTree.InOrder(mt.dataBTree.root)
			keyValuePairs := mt.dataBTree.InOrder(mt.dataBTree.root)
			keysOnly := make([][]byte, len(keyValuePairs))
			for i, pair := range keyValuePairs {
				keysOnly[i] = pair[0]
			}
			for i := range keysOnly {
				bloomfilter.Insert(string(keysOnly[i]))
			}
			return nil
		}
		return nil
	*/

	return nil
}

/*
func main() {
	mt, err := NewMemtable()
	if err != nil {
		fmt.Print(err)
	}

	mt.dataType = "hash_map"

	mt.Insert([]byte("a"), []byte("aaa"), false)
	mt.Insert([]byte("b"), []byte("bbb"), false)
	mt.Insert([]byte("c"), []byte("ccc"), false)
	mt.Insert([]byte("d"), []byte("ddd"), false)
	mt.Insert([]byte("e"), []byte("eee"), false)

	mt.PrintMemtable()

	mt.Delete([]byte("a"))
	mt.Delete([]byte("f"))

	mt.PrintMemtable()

	data1, err := mt.Get([]byte("a"))
	fmt.Print("\n", data1)

	data2, err := mt.Get([]byte("f"))
	fmt.Print("\n", data2)
}
*/
