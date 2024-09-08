package strukture

import (
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
)

const (
	HASH_MAP  = 1
	SKIP_LIST = 2
	B_TREE    = 3
)

type Memtable struct {
	size          int64
	currentSize   int64
	threshold     float64
	structureUsed int64

	hashMap  map[string]Entry
	skipList *SkipList
	bTree    *BTree
}

// Dodati ispravne parametre u konstruktore SkipList i BTree
func CreateMemtable(size, structureUsed, skipListDepth, bTreeDegree int64, threshold float64) *Memtable {
	return &Memtable{size, 0, threshold, structureUsed, map[string]Entry{}, CreateSkipList(skipListDepth), CreateBTree(bTreeDegree)}
}

func (memtable *Memtable) Insert(entry *Entry) bool {
	success := false

	if memtable.structureUsed == HASH_MAP {
		memtable.hashMap[entry.key] = *entry
		memtable.currentSize++
		success = true
	} else if memtable.structureUsed == SKIP_LIST {
		ok := memtable.skipList.Get(entry.key)
		if ok != nil {
			memtable.skipList.Delete(entry.key)
		}

		success = memtable.skipList.Insert(*entry) // Napravi da bool return value zaista radi nesto, a ne samo return true na kraju
		memtable.currentSize++
	} else if memtable.structureUsed == B_TREE {
		_, ok := memtable.bTree.Search(entry.key)
		if !ok {
			memtable.bTree.Delete(entry.key)
		}

		success, _ = memtable.bTree.Insert(*entry) // Napravi da bool return value zaista radi nesto, a ne samo return true na kraju
		memtable.currentSize++
	}

	return success
}

func (memtable Memtable) Get(key string) *Entry {
	if memtable.structureUsed == HASH_MAP {
		entry, ok := memtable.hashMap[key]
		if ok {
			return &entry
		}
	} else if memtable.structureUsed == SKIP_LIST {
		entry := memtable.skipList.Get(key)
		if entry != nil {
			return entry
		}
	} else if memtable.structureUsed == B_TREE {
		entry, ok := memtable.bTree.Search(key)
		if ok {
			return entry
		}
	}
	return nil
}

func (memtable *Memtable) Print() {
	fmt.Print("\nSize: ", memtable.size, "\nCurrent size: ", memtable.currentSize, "\n")

	if memtable.structureUsed == HASH_MAP {
		for index, data := range memtable.hashMap {
			fmt.Print("\n", index, ": ", data)
		}
	}
	if memtable.structureUsed == SKIP_LIST {
		memtable.skipList.Print()
	}
	if memtable.structureUsed == B_TREE {
		memtable.bTree.PrintTree(memtable.bTree.root, 1)
	}
}

func (memtable *Memtable) Empty(skipListDepth, bTreeDegree int64) {
	memtable.hashMap = make(map[string]Entry)
	memtable.skipList = CreateSkipList(skipListDepth)
	memtable.bTree = CreateBTree(bTreeDegree)

	memtable.currentSize = 0
}

func (memtable *Memtable) IsFull() bool {
	return (memtable.currentSize == memtable.size) || (float64(memtable.currentSize*100) >= float64(memtable.size)*memtable.threshold)
}

func (memtable *Memtable) GetSortedEntries() []string {
	entries := make([]string, 0)

	if memtable.structureUsed == HASH_MAP {
		for key := range memtable.hashMap {
			entries = append(entries, key)
		}
	}
	if memtable.structureUsed == SKIP_LIST {
		node := memtable.skipList.head

		for memtable.skipList.head != nil {
			entries = append(entries, node.key)
			node = node.down
		}
	}
	if memtable.structureUsed == B_TREE {
		for _, value := range memtable.bTree.InOrder(memtable.bTree.root) {
			entries = append(entries, value[0])
		}
	}

	slices.Sort(entries)

	return entries
}

func GetSSTableIndex() (int64, error) {
	var maxIndex int64 = -1

	fileTypes := []string{"data", "filter", "index", "summary", "metadata"}

	for _, fileType := range fileTypes {
		err := os.MkdirAll("./data/"+fileType, os.ModePerm)
		if err != nil {
			return 0, err
		}
	}

	for _, fileType := range fileTypes {
		files, _ := os.ReadDir("./data/" + fileType)

		for _, file := range files {
			fileName := file.Name()
			indexInName := strings.Split(fileName, fileType+"_")[1]
			indexInName = strings.Split(indexInName, ".bin")[0]

			index, _ := strconv.ParseInt(string(indexInName), 10, 64)

			maxIndex = max(index, maxIndex)
		}
	}

	if maxIndex == -1 {
		return 0, nil
	}
	return maxIndex + 1, nil
}

func CreateFiles(fileIndex int64) error {
	dataFilePath := "./data/data/data_" + fmt.Sprint(fileIndex) + ".bin"
	filterFilePath := "./data/filter/filter_" + fmt.Sprint(fileIndex) + ".bin"
	indexFilePath := "./data/index/index_" + fmt.Sprint(fileIndex) + ".bin"
	summaryFilePath := "./data/summary/summary_" + fmt.Sprint(fileIndex) + ".bin"
	metadataFilePath := "./data/metadata/metadata_" + fmt.Sprint(fileIndex) + ".bin"

	_, err := os.Create(dataFilePath)
	if err != nil {
		return err
	}
	_, err = os.Create(filterFilePath)
	if err != nil {
		return err
	}
	_, err = os.Create(metadataFilePath)
	if err != nil {
		return err
	}
	_, err = os.Create(indexFilePath)
	if err != nil {
		return err
	}
	_, err = os.Create(summaryFilePath)
	if err != nil {
		return err
	}

	return nil
}

func (memtable *Memtable) Flush(bloomFilterExpectedElements, indexDensity, summaryDensity int64, bloomFilterFalsePositiveRate float64) error {
	fileIndex, err := GetSSTableIndex()
	if err != nil {
		return err
	}

	err = CreateFiles(fileIndex)
	if err != nil {
		return err
	}
	dataFile, err := os.OpenFile("./data/data/data_"+fmt.Sprint(fileIndex)+".bin", os.O_APPEND, 0777)
	if err != nil {
		return err
	}
	filterFile, err := os.OpenFile("./data/filter/filter_"+fmt.Sprint(fileIndex)+".bin", os.O_APPEND, 0777)
	if err != nil {
		return err
	}
	indexFile, err := os.OpenFile("./data/index/index_"+fmt.Sprint(fileIndex)+".bin", os.O_APPEND, 0777)
	if err != nil {
		return err
	}
	summaryFile, err := os.OpenFile("./data/summary/summary_"+fmt.Sprint(fileIndex)+".bin", os.O_APPEND, 0777)
	if err != nil {
		return err
	}
	metadataFile, err := os.OpenFile("./data/metadata/metadata_"+fmt.Sprint(fileIndex)+".bin", os.O_APPEND, 0777)
	if err != nil {
		return err
	}

	sortedKeys := memtable.GetSortedEntries()
	bloomFilter := NewBloomFilterWithSize(bloomFilterExpectedElements, bloomFilterFalsePositiveRate)
	merkleTree := NewMerkleTree()
	indexData := make(map[string]int64)
	summaryData := make(map[string]int64)

	var memtableSize int64 = 0
	// Serialize data
	for i, key := range sortedKeys {
		entry := memtable.Get(key)
		if entry == nil {
			continue
		}

		length, err := dataFile.Write(entry.ToByteArray())
		if err != nil {
			return err
		}

		bloomFilter.Insert(key)
		merkleTree.AddElement(key, *entry)

		if int64(i)%indexDensity == 0 {
			indexData[key] = memtableSize

			/*
				if int64(i) % (indexDensity * summaryDensity) == 0 {
					summaryData[key] = indexSize
				}
				indexSize += 2 + int64(len(key))
			*/
		}
		memtableSize += int64(length)
	}

	// Serialize filter
	_, err = filterFile.Write(bloomFilter.Serialize())
	if err != nil {
		return err
	}

	// Serialize index
	var indexSize int64 = 0
	sortedIndexKeys := make([]string, 0)

	for key := range indexData {
		sortedIndexKeys = append(sortedIndexKeys, key)
	}
	//sort.Strings(sortedIndexKeys)

	for i, key := range sortedIndexKeys {
		if int64(i)%summaryDensity == 0 {
			summaryData[key] = indexSize
		}
		indexSize += 2 + int64(len(key))

		writeToIndex := make([]byte, 8)
		binary.BigEndian.PutUint64(writeToIndex, uint64(len(key)))
		indexFile.Write(writeToIndex)

		indexFile.Write([]byte(key))

		writeToIndex = make([]byte, 8)
		binary.BigEndian.PutUint64(writeToIndex, uint64(indexData[key]))
		indexFile.Write(writeToIndex)
	}

	// Serialize summary
	sortedSummaryKeys := make([]string, 0)

	for key := range summaryData {
		sortedSummaryKeys = append(sortedSummaryKeys, key)
	}
	sort.Strings(sortedSummaryKeys)

	for _, key := range sortedSummaryKeys {
		writeToSummary := make([]byte, 8)
		binary.BigEndian.PutUint64(writeToSummary, uint64(len(key)))
		summaryFile.Write(writeToSummary)

		summaryFile.Write([]byte(key))

		writeToSummary = make([]byte, 8)
		binary.BigEndian.PutUint64(writeToSummary, uint64(summaryData[key]))
		summaryFile.Write(writeToSummary)
	}

	// Serialize metadata
	merkleTree.CreateTreeWithElems()
	metadataFile.Write(merkleTree.SerializeTree())

	dataFile.Close()
	indexFile.Close()
	summaryFile.Close()
	filterFile.Close()
	metadataFile.Close()

	return nil
}
func (memtable *Memtable) Find(key string) *Entry {
	if memtable.structureUsed == HASH_MAP {
		entry, ok := memtable.hashMap[key]
		if ok {
			return &entry
		}
	} else if memtable.structureUsed == SKIP_LIST {
		entry := memtable.skipList.Get(key)
		if entry != nil {
			return entry
		}
	} else if memtable.structureUsed == B_TREE {
		entry, ok := memtable.bTree.Search(key)
		if ok {
			return entry
		}
	}
	return nil
}

/* Ne znam sto nam je delete uopste potreban?

func (memtable *Memtable) Delete(key string) error {
	if memtable.structureUsed == HASH_MAP {
		entry, ok := memtable.hashMap[key]
		if ok {
			return &entry
		}

		delete(memtable.hashMap, key)

	} else if memtable.structureUsed == SKIP_LIST {
		entry := memtable.skipList.Get(key)
		if entry != nil {
			return entry
		}
	} else if memtable.structureUsed == B_TREE {
		entry, ok := memtable.bTree.Search(key)
		if ok {
			return entry
		}
	}
	return nil

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

*/

/* Mislim da ovo ne treba
func (mt *Memtable) Exists(key []byte) bool {
	switch mt.dataType {
	case "skip_list":
		if mt.dataSkipList.Get(key) == nil {
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
*/

/*
func readFiles(maxID int, key string) {
	folderPaths := []string{"filter", "summary", "index", "sstable"}
	fmt.Println(folderPaths)
	for i := 1; i <= maxID; i++ {
		for _, folderPath := range folderPaths {
			fileName := fmt.Sprintf("%s_1_%d.db", folderPath, i)

			file, err := openFileInFolder(folderPath, fileName)
			if err != nil {
				fmt.Printf("a")
				return
			}

			content, errFile := ioutil.ReadAll(file)
			if errFile != nil {
				fmt.Printf("b")
				return
			}

			bf, errBf := DeserializeBloomFilter(content)
			if errBf != nil {
				fmt.Printf("c")
				return
			}
			// deserijalizovan bloom filter
			isInBloomFilter := bf.Lookup(key)
			if !isInBloomFilter {
				break
			}
			folderSummary := folderPaths[1]
			fileSummary := fmt.Sprintf("%s_1_%d.db", folderSummary, i)

			summaryFile, errSummary := os.Open(fileSummary)
			if errSummary != nil {
				fmt.Printf("Error opening summary file: %s\n", errSummary)
				return
			}
			// nisam siguran kako izgleda summary file
			buffer := make([]byte, 4)
			_, errRead := io.ReadFull(summaryFile, buffer)
			if errRead != nil {
				fmt.Printf("Error reading from summary file: %s\n", errRead)
				return
			}

			// Store the first 4 bytes into a variable
			// kljuc := buffer
			folderIndex := folderPaths[2]
			fileIndex := fmt.Sprintf("%s_1_%d.db", folderIndex, i)

			indexFile, errIndex := os.Open(fileIndex)
			if errIndex != nil {
				fmt.Printf("Error opening summary file: %s\n", errIndex)
				return
			}
			// nisam siguran kako izgleda summary file
			buffer2 := make([]byte, 4)
			_, errRead2 := io.ReadFull(indexFile, buffer2)
			if errRead2 != nil {
				fmt.Printf("Error reading from summary file: %s\n", errRead2)
				return
			}
		}
	}
}

func openFileInFolder(folderPath, fileName string) (*os.File, error) {
	filePath := filepath.Join(folderPath, fileName)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Opened file: %s\n", filePath)

	return file, nil
}

func openFolder(folderPath string) error {
	err := os.MkdirAll(folderPath, os.ModePerm)
	if err != nil {
		return err
	}

	// fmt.Printf("Otvoren folder: %s\n", folderPath)

	return nil
}

// Fali varijabilni enkoding i prosljedjivanje puta fajlova parametrom. Mislim da i racunanje vrijednosti koje se zapisuju i index summary isto ne radi.
// Molim onoga ko je radio sa varijabilnim enkodingom da drugom mjestu da ga doda i ovdje. 🙏
// Osim toga, trebalo bi da je Flush zavrsen do kraja, sto ukljucuje tacke, podtacke, dodatne zahtjeve...

func MergeSSTable(numEntries []int, sstableFiles []*os.File) error {

	entries := make(map[string]MemtableEntry)

	for i, sstableFile := range sstableFiles {
		numEntry := numEntries[i]

		// Extract lsmLevel from the file name
		lsmLevel, index, err := extractLSMLevelAndIndex(sstableFile.Name())
		if err != nil {
			return err
		}

		for j := 0; j < numEntry; j++ {
			content, err := ioutil.ReadAll(sstableFile)
			if err != nil {
				return nil
			}
			entry, bytesRead, err := DeserializeMemtableEntry(content)
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			existingEntry, exists := entries[string(entry.Key)]
			if !exists || entry.Timestamp.After(existingEntry.Timestamp) {
				entries[string(entry.Key)] = entry
			}

			_, err = sstableFile.Seek(int64(bytesRead), io.SeekCurrent)
			if err != nil {
				return err
			}
		}

		sstableFile.Close()

		err = DeleteSSTableFiles(lsmLevel, index)
		if err != nil {
			fmt.Println("Error deleting SSTable files:", err)
		}
	}

	var sortedKeys []string
	for key := range entries {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	memtable, err := NewMemtable(10000, 10, 10, "b_tree")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		err := memtable.Insert(&entry)
		if err != nil {
			return err
		}
	}
	memtable.Flush(2, 2, 1, 5000, 0.2, true)
	memtable.Flush(2, 2, 1, 5000, 0.2, false)

	return nil
}
func DeleteSSTableFiles(lsmLevel, index int) error {
	rootDir := "./"
	fileTypes := []string{"data", "index", "filter", "summary", "metadata", "sstable"}

	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		for _, fileType := range fileTypes {
			fileName := fmt.Sprintf("%s_%d_%d.db", fileType, lsmLevel, index)
			if strings.HasSuffix(info.Name(), fileName) {
				err := os.Remove(path)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func extractLSMLevelAndIndex(fileName string) (int, int, error) {
	parts := strings.Split(fileName, "_")
	if len(parts) != 3 {
		return 0, 0, errors.New("invalid file name format")
	}

	lsmLevel, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}

	index, err := strconv.Atoi(strings.TrimSuffix(parts[2], ".db"))
	if err != nil {
		return 0, 0, err
	}
	return lsmLevel, index, nil
}

/*
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

	mt.Flush(2, 2, 1, 5000, 0.2, true)

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
*/
