package strukture

import (
	MerkleTree "NASP_projekat2023/strukture/MerkleTree"
	hashfunc "NASP_projekat2023/utils"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
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

func (mt *Memtable) GetSortedEntries() []string {
	entries := make([]string, 0)

	if mt.dataType == "hash_map" {
		for key := range mt.dataHashMap {
			entries = append(entries, key)
		}
	}
	if mt.dataType == "skip_list" {
		node := mt.dataSkipList.head

		for mt.dataSkipList.head != nil {
			entries = append(entries, string(node.key))
			node = node.down
		}
	}
	if mt.dataType == "b_tree" {
		for _, value := range mt.dataBTree.InOrder(mt.dataBTree.root) {
			entries = append(entries, string(value[0]))
		}
	}
	sort.Strings(entries)

	return entries
}

func GetSSTableIndex(lsm_level int) int {
	maxIndex := 0
	fileTypes := []string{"sstable", "index", "filter", "summary", "metadata"}

	for _, fileType := range fileTypes {
		files, _ := os.ReadDir("./" + fileType)

		for _, f := range files {
			fileName := f.Name()
			fileRegex := fileType + "_" + fmt.Sprint(lsm_level) + "_\\d+.db"

			match, _ := regexp.Match(fileRegex, []byte(fileName))

			if match {
				index := strings.Split(fileName, fileType+"_")
				index = strings.Split(index[1], ".db")
				index = strings.Split(index[0], "_")
				id, _ := strconv.Atoi(index[1])

				maxIndex = max(id, maxIndex)
			}
		}
	}
	return maxIndex + 1
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

func (mt *Memtable) Flush(indexSparsity, summarySparsity, lsmLevel int, multipleFiles bool) error {
	if multipleFiles {
		sortedKeys := mt.GetSortedEntries()
		index := GetSSTableIndex(lsmLevel)
		bf := NewBloomFilterWithSize(50000, 0.2)
		mtree := MerkleTree.NewMerkleTree()
		tableIndex := make(map[string]uint64)
		summaryIndex := make(map[string]uint64)
		last := ""
		var totalMemtableSize uint64 = 0
		var totalIndexSize uint64 = 0

		dataFile, err := os.Create("./sstable/sstable" + fmt.Sprint(lsmLevel) + "_" + fmt.Sprint(index) + ".db")
		if err != nil {
			return err
		}
		defer dataFile.Close()
		filterFile, err := os.Create("./filter/filter_" + fmt.Sprint(lsmLevel) + "_" + fmt.Sprint(index) + ".db")
		if err != nil {
			return err
		}
		defer filterFile.Close()
		metadataFile, err := os.Create("./metadata/metadata_" + fmt.Sprint(lsmLevel) + "_" + fmt.Sprint(index) + ".db")
		if err != nil {
			return err
		}
		defer metadataFile.Close()
		indexFile, err := os.Create("./index/index_" + fmt.Sprint(lsmLevel) + "_" + fmt.Sprint(index) + ".db")
		if err != nil {
			return err
		}
		defer indexFile.Close()
		summaryFile, err := os.Create("./summary/summary_" + fmt.Sprint(lsmLevel) + "_" + fmt.Sprint(index) + ".db")
		if err != nil {
			return err
		}
		defer summaryFile.Close()

		for i, key := range sortedKeys {
			entry, err := mt.Get([]byte(key))
			if err != nil {
				return err
			}

			bf.Insert(key)
			mtree.AddElement([]byte(key))

			serializedEntry := SerializeMemtableEntry(*entry)
			_, err = dataFile.Write(serializedEntry)

			if i%indexSparsity == 0 {
				tableIndex[key] = totalMemtableSize

				if i%(indexSparsity*summarySparsity) == 0 {
					summaryIndex[key] = totalIndexSize
				}

				totalIndexSize += uint64(len(fmt.Sprint(len(key))))
				totalIndexSize += uint64(len(key))
				totalIndexSize += uint64(len(fmt.Sprint(tableIndex[key]))) // Mislim da se ovdje nalazi neka greska? Sad sam preumoran da je nadjem
				last = key
			}
			totalMemtableSize += uint64(len(serializedEntry))

			if err != nil {
				return err
			}
		}

		// Serialize bloom filter
		_, err = filterFile.Write(SerializeBloomFilter(bf))
		if err != nil {
			return err
		}

		// Serialize merkle tree
		metadataFile.Write(mtree.SerializeTree())

		// Serialize table index
		indexEntries := make([]string, 0)
		for key := range tableIndex {
			indexEntries = append(indexEntries, key)
		}
		sort.Strings(indexEntries)

		for _, key := range indexEntries {
			indexFile.Write([]byte{byte(len(key))})
			indexFile.Write([]byte(key))
			indexFile.Write([]byte(fmt.Sprint(tableIndex[key]))) // Ovdje valjda fali varijabilni enkoding
		}

		// Serialize index summary
		summaryFile.Write([]byte{byte(len(indexEntries[0]))}) // Ovdje mozda treba varijabilni enkoding
		summaryFile.Write([]byte(indexEntries[0]))
		summaryFile.Write([]byte{byte(len(last))})
		summaryFile.Write([]byte(last))

		summaryEntries := make([]string, 0)
		for key := range summaryIndex {
			summaryEntries = append(summaryEntries, key)
		}
		sort.Strings(summaryEntries)

		for _, key := range summaryEntries {
			summaryFile.Write([]byte{byte(len(key))})
			summaryFile.Write([]byte(key))
			summaryFile.Write([]byte(fmt.Sprint(tableIndex[key]))) // Ovdje valjda fali varijabilni enkoding
		}
	}

	return nil
}

func main() {
	mt, err := NewMemtable(100, 10, 10, "b_tree")
	if err != nil {
		fmt.Print(err)
	}

	a := NewMemtableEntry([]byte("a"), []byte("aaa"), false, time.Now())
	b := NewMemtableEntry([]byte("b"), []byte("bbbb"), true, time.Now())
	c := NewMemtableEntry([]byte("c"), []byte("ccccc"), false, time.Now())
	d := NewMemtableEntry([]byte("d"), []byte("dddd"), false, time.Now())
	e := NewMemtableEntry([]byte("e"), []byte("eee"), false, time.Now())

	mt.Insert(a)
	mt.Insert(b)
	mt.Insert(c)
	mt.Insert(d)
	mt.Insert(e)

	// aa := SerializeMemtableEntry(*b)
	// aaa := DeserializeMemtableEntry(aa)
	// fmt.Print(aaa)

	mt.Flush(2, 2, 1, true)

	// mt.PrintMemtable()

	mt.Delete([]byte("a"))
	mt.Delete([]byte("f"))

	// mt.Flush()

	// mt.PrintMemtable()

	data1, err := mt.Get([]byte("a"))
	fmt.Print("\n", data1)

	data2, err := mt.Get([]byte("f"))
	fmt.Print("\n", data2)
}
