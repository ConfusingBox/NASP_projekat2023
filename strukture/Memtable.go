package strukture

import (
	"errors"
	"fmt"
	"time"

	config "NASP_projekat2023/utils"
)

type MemtableEntry struct {
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Tombstone bool
}

func NewMemtableEntry(key, value []byte, tombstone bool) *MemtableEntry {
	return &MemtableEntry{key, value, time.Now(), tombstone}
}

func EmptyMemtableEntry(key []byte) ([]byte, []byte, bool) {
	return key, nil, true
}

type Memtable struct {
	size        int
	currentSize int

	dataType     string
	dataHashMap  map[string]MemtableEntry
	dataSkipList *SkipList
	dataBTree    *BTree
}

func NewMemtable() (*Memtable, error) {
	config, err := config.LoadConfig("config.json")
	if err != nil {
		return nil, err
	}

	mt := Memtable{config.MemTableSize, 0, config.MemTableType, make(map[string]MemtableEntry), NewSkipList(config.SkipListDepth), NewBTree(config.BTreeDegree)}
	return &mt, nil
}

func (mt *Memtable) Insert(key, value []byte, tombstone bool) error {
	entry := NewMemtableEntry(key, value, tombstone)

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

	if mt.currentSize >= mt.size {
		mt.Flush()
		mt.currentSize = 0
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
	}

	err := mt.Insert(EmptyMemtableEntry(key))

	if err != nil {
		return err
	}

	return nil
}

func (mt *Memtable) DeleteBTree(key []byte) error {
	success := mt.dataBTree.Delete(key)

	if success {
		mt.currentSize--
	}

	err := mt.Insert(EmptyMemtableEntry(key))

	if err != nil {
		return err
	}

	return nil
}

func (mt *Memtable) DeleteHashMap(key []byte) error {
	_, exist := mt.dataHashMap[string(key)]

	if exist {
		delete(mt.dataHashMap, string(key))
		mt.currentSize--
	}

	err := mt.Insert(EmptyMemtableEntry(key))

	if err != nil {
		return err
	}

	return nil
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
