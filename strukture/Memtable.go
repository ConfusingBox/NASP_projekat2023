package strukture

import (
	MerkleTree "NASP_projekat2023/strukture/MerkleTree"
	hashfunc "NASP_projekat2023/utils"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
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

func GetSSTableIndex(lsmLevel int) int {
	maxIndex := 0
	fileTypes := make([]string, 0)

	fileTypes = []string{"data", "index", "filter", "summary", "metadata", "sstable"}

	for _, fileType := range fileTypes {
		files, _ := os.ReadDir("./" + fileType)

		for _, f := range files {
			fileName := f.Name()
			fileRegex := fileType + "_" + fmt.Sprint(lsmLevel) + "_\\d+.db"

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
			kljuc := buffer
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

// Fali varijabilni enkoding i prosljedjivanje puta fajlova parametrom. Mislim da i racunanje vrijednosti koje se zapisuju i index summary isto ne radi.
// Molim onoga ko je radio sa varijabilnim enkodingom da drugom mjestu da ga doda i ovdje. 🙏
// Osim toga, trebalo bi da je Flush zavrsen do kraja, sto ukljucuje tacke, podtacke, dodatne zahtjeve...
func (mt *Memtable) Flush(indexSparsity, summarySparsity, lsmLevel, bloomFilterExpectedElements int, bloomFilterFalsePositiveRate float64, multipleFiles bool) error {
	sortedKeys := mt.GetSortedEntries()
	bf := NewBloomFilterWithSize(bloomFilterExpectedElements, bloomFilterFalsePositiveRate)
	mtree := MerkleTree.NewMerkleTree()
	tableIndex := make(map[string]uint64)
	summaryIndex := make(map[string]uint64)
	last := ""
	index := GetSSTableIndex(lsmLevel)
	var totalMemtableSize uint64 = 0
	var totalIndexSize uint64 = 0

	// Put za fajlove se valjda treba proslijediti kao parametar. Ko zna kako ce to izgledati moze da doda parametar i izmjeni ovdje puteve. Ako mijenjate sablon imenovanja fajla,
	// onda izmjenite i regex u funkciji GetSSTableIndex.
	dataFilePath := "./data/data" + fmt.Sprint(lsmLevel) + "_" + fmt.Sprint(index) + ".db"
	filterFilePath := "./filter/filter_" + fmt.Sprint(lsmLevel) + "_" + fmt.Sprint(index) + ".db"
	metadataFilePath := "./metadata/metadata_" + fmt.Sprint(lsmLevel) + "_" + fmt.Sprint(index) + ".db"
	indexFilePath := "./index/index_" + fmt.Sprint(lsmLevel) + "_" + fmt.Sprint(index) + ".db"
	summaryFilePath := "./summary/summary_" + fmt.Sprint(lsmLevel) + "_" + fmt.Sprint(index) + ".db"
	sstableFilePath := "./sstable/sstable" + fmt.Sprint(lsmLevel) + "_" + fmt.Sprint(index) + ".db"

	// OBJASNJENJE IMENA FAJLOVA (jer znam da ce biti zabune...)
	//
	// data		- Cuva serijalizovane podatke jedne Memtabele. To je, dakle, jedan SSTable/SSTable zapis
	// 			  Pojedinacan entry je serijalizovan u formatu   -   crc - timestamp - tombstone - key size - value size - key - value
	//			  Ako je tombstone == true, onda su value size i value polja izostavljeni.
	// filter	- Cuva serijalizovan BloomFilter koji odgovara samo jednoj Memtabeli
	// 			  Ne znam u kojem formatu je serijalizovan filter. To se nalazi u BloomFilter fajlu i mislim da ga je najbolje koristiti tako da se citav fajl procita,
	//			  ti podaci deserijalizuju u novi BloomFilter, a zatim se on koristi.
	// metadata - Cuva serijalizovan MerkleTree koji odgovara samo jednoj Memtabeli
	// 			  Za formatiranje fajla vazi isto sto sam rekao i za BloomFilter.
	// index	- Cuva index podatke koji odgovaraju samo jednoj Memtabeli
	// 			  U fajl se redom zapisuju sljedeci podaci - duzina kljuca, kljuc, offset. Offset predstavlja mjesto u data fajlu, relativno na pocetak tog data fajla,
	//			  na kojem se nalazi entry koji odgovara kljucu kod tog offseta.
	// summary	- Cuva summary podatke koji odgovaraju samo jednom indexu
	// 			  U fajl su prvo zapisani sljedeci podaci - duzina prvog kljuca koji je zapisan u index fajlu, prvi kljuc koji je zapisan u index fajlu,
	//			  duzina posljednjeg kljuca koji je zapisan u index fajlu, posljednji kljuc koji je zapisan u index fajlu.
	// 			  Nakon toga, redom se pisu podaci - duzina kljuc, kljuc, offset. Offset predstavlja mjesto u index fajlu, relativno na pocetak tog index fajla,
	//			  na kojem se nalazi podatak o mjestu tog kljuca u data fajlu.
	// sstable	- U jednom fajlu cuva serijalizovane podatke jedne Memtabele, kao i sve popratne strukture koje joj odgovaraju - BloomFilter, MerkleTree, index i summary.
	// 			  Drugim rijecima, to je prethodnih pet fajlova zapisanih u jedan fajl.
	//			  Pravi se kada se multipleFiles prosljedjena vrijednost jednaka false.
	// 			  U sstable fajl, podaci iz ostalih fajlova se zapisuju sljedecim redosljedom - filter, summary, index, data, metadata.
	//			  Podaci u njima su identicnog formata kao i kada se zapisuju zajedno.
	//			  Razlika je u tome sto se prije svakog dijela nalazi broj koji govori duzinu tog dijela. Dakle - duzina filter podataka, filter podaci, duzina summary podataka, summary podaci...
	//			  Stoga, ako zelite da citate data npr, prvo procitate prvi broj (koji predstavlja duzinu filter dijela), pa skocite za toliko bajtova unaprijed,
	//			  pa citate sljedeci broj i skacete toliko bajtova, pa onda to uradite jos jednom da preskocite i index dio.
	//			  Nakon toga, trebalo bi da se nalazite kod broja koji govori duzinu data dijela. Procitajte ga i onda se u sljedecih toliko bajtova nalazi citav data dio.

	dataFile, err := os.Create(dataFilePath)
	if err != nil {
		return err
	}
	filterFile, err := os.Create(filterFilePath)
	if err != nil {
		return err
	}
	metadataFile, err := os.Create(metadataFilePath)
	if err != nil {
		return err
	}
	indexFile, err := os.Create(indexFilePath)
	if err != nil {
		return err
	}
	summaryFile, err := os.Create(summaryFilePath)
	if err != nil {
		return err
	}

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

	// Multiple files == false
	if !multipleFiles {
		sstableFile, err := os.OpenFile(sstableFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		defer sstableFile.Close()

		for _, file := range []*os.File{filterFile, summaryFile, indexFile, dataFile, metadataFile} {
			stat, err := file.Stat()
			if err != nil {
				return errors.New("Greska pri citanju fajla " + stat.Name())
			}
			sstableFile.Write([]byte(fmt.Sprint(stat.Size()))) // Ovdje valjda fali varijabilni enkoding

			data, err := io.ReadAll(file)
			if err != nil {
				return errors.New("Greska pri zapisivanju fajla " + stat.Name())
			}
			sstableFile.Write(data)
		}
	}
	dataFile.Close()
	indexFile.Close()
	summaryFile.Close()
	filterFile.Close()
	metadataFile.Close()

	if !multipleFiles {
		os.Remove(dataFilePath)
		os.Remove(indexFilePath)
		os.Remove(summaryFilePath)
		os.Remove(filterFilePath)
		os.Remove(metadataFilePath)
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
