// -Potrebno je omoguciti da korisnik podesava osobine memtable-a. Za to nam fali config.json fajl.
// -Potrebno je omoguciti i implementaciju sa skip listom. Za to nam fali SkipList.go fajl.

// Da li entry treba da bude tip value-a?

package strukture

import (
	"errors"
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
	threshold   float32
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

	mt := Memtable{config.MemTableThreshold, config.MemTableSize, 0, config.MemTableType, make(map[string]MemtableEntry), NewSkipList(config.SkipListDepth), NewBTree(config.BTreeDegree)}
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

	if mt.threshold*float32(mt.size) <= 100.0*float32(mt.currentSize) {
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

/*
	func (mt *Memtable) InsertBTree(entry *MemtableEntry) error {
		err := mt.dataBTree.Insert(entry)

		if err != nil {
			return err
		}

		mt.currentSize += 1
		return nil
	}
*/
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

	} else {
		err := mt.Insert(EmptyMemtableEntry(key))

		if err != nil {
			return err
		}

		mt.currentSize++
	}

	return nil
}

func (mt *Memtable) DeleteBTree(key []byte) error {
	success := mt.dataBTree.Delete(key)

	if success {
		mt.currentSize--

	} else {
		err := mt.Insert(EmptyMemtableEntry(key))

		if err != nil {
			return err
		}

		mt.currentSize++
	}

	return nil
}

func (mt *Memtable) DeleteHashMap(key []byte) error {
	_, exist := mt.dataHashMap[string(key)]

	if exist {
		delete(mt.dataHashMap, string(key))
		mt.currentSize--

	} else {
		err := mt.Insert(EmptyMemtableEntry(key))

		if err != nil {
			return err
		}

		mt.currentSize++
	}

	return nil
}

func (mt *Memtable) Get(key []byte) ([]byte, error) {
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

func (mt *Memtable) GetSkipList(key []byte) ([]byte, error) {
	exist := mt.dataSkipList.Search(key)

	if exist == nil {
		return nil, errors.New("Zapis ne postoji.")
	}

	return exist.entry.Value, nil
}

func (mt *Memtable) GetBTree(key []byte) ([]byte, error) {
	value, exist := mt.dataBTree.Search(key)

	if !exist {
		return nil, errors.New("Zapis ne postoji.")
	}

	return value, nil
}

func (mt *Memtable) GetHashMap(key []byte) ([]byte, error) {
	value, exist := mt.dataHashMap[string(key)]

	if !exist {
		return nil, errors.New("Zapis ne postoji.")
	}

	return value.Value, nil
}

func (mt *Memtable) Flush() error {
	// Poziva se kada treshold >= size. (Moze li biti vece ili mora striktno jednako?
	return nil
}

/*
func initializeMemtable() {
	// Kada se sistem pokrene, Memtable treba popuniti zapisima iz WAL-a.
}
*/
