// -Potrebno je omoguciti da korisnik podesava osobine memtable-a. Za to nam fali config.json fajl.
// -Potrebno je omoguciti i implementaciju sa skip listom. Za to nam fali SkipList.go fajl.

// Da li entry treba da bude tip value-a?

package strukture

import (
	"errors"
	"time"

	config "NASP_projekat2023/utils"
)

type EmptyEntry struct {
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Tombstone bool
}

type Memtable struct {
	threshold   float32
	size        int
	currentSize int

	dataType     string
	dataHashMap  map[string][]byte
	dataSkipList *SkipList
	dataBTree    *BTree
}

func NewMemtable() (*Memtable, error) {
	config, err := config.LoadConfig("config.json")
	if err != nil {
		return nil, err
	}

	mt := Memtable{config.MemTableThreshold, config.MemTableSize, 0, config.MemTableType, make(map[string][]byte), NewSkipList(config.SkipListDepth), NewBTree(config.BTreeDegree)}
	return &mt, nil
}

func (mt *Memtable) Insert(key string, value []byte) error {
	if mt.dataType == "skip_list" {
		err := mt.InsertSkipList(key, value)
		if err != nil {
			return err
		}

	} else if mt.dataType == "b_tree" {
		err := mt.InsertBTree(key, value)
		if err != nil {
			return err
		}

	} else if mt.dataType == "hash_map" {
		err := mt.InsertHashMap(key, value)
		if err != nil {
			return err
		}

	} else {
		return errors.New("Los naziv strukture kod Memtable.Insert().")
	}

	if mt.threshold*float32(mt.size) <= 100*float32(mt.currentSize) {
		mt.Flush()
		mt.currentSize = 0
	}

	return nil
}

func (mt *Memtable) InsertSkipList(key string, value []byte) error {
	exist := mt.dataSkipList.Search([]byte(key))

	if exist == nil {
		mt.dataSkipList.Insert([]byte(key), value)
		mt.currentSize += 1

		return nil
	}
	return errors.New("Same key already here lol")
}

func (mt *Memtable) InsertBTree(key string, value []byte) error {
	err := mt.dataBTree.Insert([]byte(key), value)

	if err != nil {
		return err
	}

	mt.currentSize += 1
	return nil
}

func (mt *Memtable) InsertHashMap(key string, value []byte) error {
	_, exist := mt.dataHashMap[key]

	if exist {
		return errors.New("Same key already here lol")
	}

	mt.dataHashMap[key] = value
	mt.currentSize += 1
	return nil
}

func (mt *Memtable) Delete(key string) error {
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

func (mt *Memtable) DeleteSkipList(key string) error {
	success := mt.dataSkipList.Delete([]byte(key))

	if success {
		mt.currentSize--

	} else {
		err := mt.Insert(key, mt.EmptyEntry(key, 0, time.Now(), true))

		if err != nil {
			return err
		}

		mt.currentSize++
	}

	return nil
}

func (mt *Memtable) DeleteBTree(key string) error {
	success := mt.dataBTree.Delete([]byte(key))

	if success {
		mt.currentSize--

	} else {
		err := mt.Insert(key, mt.EmptyEntry(key, 0, time.Now(), true))

		if err != nil {
			return err
		}

		mt.currentSize++
	}

	return nil
}

func (mt *Memtable) DeleteHashMap(key string) error {
	_, exist := mt.dataHashMap[key]

	if exist {
		delete(mt.dataHashMap, key)
		mt.currentSize--

	} else {
		err := mt.Insert(key, mt.EmptyEntry(key, 0, time.Now(), true))

		if err != nil {
			return err
		}

		mt.currentSize++
	}

	return nil
}

func (mt *Memtable) Get(key string) ([]byte, error) {
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

func (mt *Memtable) GetSkipList(key string) ([]byte, error) {
	exist := mt.dataSkipList.Search([]byte(key))

	if exist == nil {
		return nil, errors.New("Zapis ne postoji.")
	}

	return exist.value, nil
}

func (mt *Memtable) GetBTree(key string) ([]byte, error) {
	value, exist := mt.dataBTree.Search([]byte(key))

	if !exist {
		return nil, errors.New("Zapis ne postoji.")
	}

	return value, nil
}

func (mt *Memtable) GetHashMap(key string) ([]byte, error) {
	value, exist := mt.dataHashMap[key]

	if !exist {
		return nil, errors.New("Zapis ne postoji.")
	}

	return value, nil
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
