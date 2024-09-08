package main

import (
	"NASP_projekat2023/strukture"
	"NASP_projekat2023/utils"
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type Engine struct {
	Config      *utils.Config
	TokenBucket *strukture.TokenBucket
	WAL         *strukture.WriteAheadLog
	Cache       *strukture.LRUCache
	Mempool     *strukture.Mempool
}

func (engine *Engine) LoadStructures() bool {
	Config, err := utils.LoadConfigValues("config.json")
	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	Mempool := strukture.CreateMempool(Config.MemPoolSize, Config.MemTableSize, Config.MemTableType, Config.SkipListDepth, Config.BTreeDegree, Config.MemTableThreshold)
	TokenBucket := strukture.NewTokenBucket(int(Config.TokenBucketLimitSeconds), int(Config.TokenBucketCapacity))
	WAL, err1 := strukture.CreateWriteAheadLog(Config.WALSegmentSize)
	if err1 != nil {
		fmt.Println(err1.Error())
		return false
	}
	Cache := strukture.NewLRUCache(Config.CacheSize)

	*engine = Engine{
		Config:      Config,
		WAL:         WAL,
		TokenBucket: TokenBucket,
		Cache:       &Cache,
		Mempool:     Mempool,
	}

	return true
}

func (engine *Engine) Put(key string, value []byte) bool {
	if !engine.TokenBucket.Allow() {
		fmt.Println("Wait until request is available")
		return false
	}

	entry := strukture.CreateEntry(key, value, 0)

	err := engine.WAL.Log(entry)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	err = engine.Mempool.Insert(entry, engine.Config.BloomFilterExpectedElements, engine.Config.IndexDensity, engine.Config.SummaryDensity, engine.Config.SkipListDepth, engine.Config.BTreeDegree, engine.Config.BloomFilterFalsePositiveRate)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	return true
}

func (engine *Engine) Get(key string, indexDensity int64) ([]byte, bool) {
	if !engine.TokenBucket.Allow() {
		fmt.Println("Wait until request is available")
		return nil, false
	}

	if entry := engine.Mempool.Find(key); entry != nil {
		return entry.GetValue(), true
	}

	if value := engine.Cache.Get([]byte(key)); value != nil {
		return value, true
	}

	currentHighestFileIndex, err := GetCurrentHighestFileIndex()
	if err != nil {
		fmt.Println("Error loading Bloom Filter:", err)
		return nil, false
	}

	for i := currentHighestFileIndex; i >= 0; i-- {
		bloomFilter, err := loadBloomFilterFromFile("data/filter/filter_" + fmt.Sprint(i) + ".bin")
		if err != nil {
			fmt.Println("Error loading Bloom Filter:", err)
			return nil, false
		}

		if bloomFilter.Lookup(key) {
			indexFileOffset, err := findFileOffset("data/summary/summary_"+fmt.Sprint(i)+".bin", key, 0)
			if err != nil {
				fmt.Println("Error finding index offset:", err)
				return nil, false
			}
			if indexFileOffset == -1 {
				continue
			}

			dataFileOffset, err := findFileOffset("data/index/index_"+fmt.Sprint(i)+".bin", key, indexFileOffset)
			if err != nil {
				fmt.Println("Error finding data offset:", err)
				return nil, false
			}
			if dataFileOffset == -1 {
				continue
			}

			var j int64
			for j = 0; j <= indexDensity; j++ {
				value, length, err := ReadDataFile(i, dataFileOffset, key)
				if err != nil {
					return nil, false
				}

				if value != nil {
					return value, true
				}

				dataFileOffset += length
			}
		}
	}

	fmt.Println("Key not found")
	return nil, false
}

func (engine *Engine) Delete(key string) bool {
	if !engine.TokenBucket.Allow() {
		fmt.Println("Wait until request is available")
		return false
	}

	entry := strukture.CreateEntry(key, nil, 1)

	err := engine.WAL.Log(entry)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	err = engine.Mempool.Insert(entry, engine.Config.BloomFilterExpectedElements, engine.Config.IndexDensity, engine.Config.SummaryDensity, engine.Config.SkipListDepth, engine.Config.BTreeDegree, engine.Config.BloomFilterFalsePositiveRate)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	if value := engine.Cache.Get([]byte(key)); value != nil {
		engine.Cache.Remove([]byte(key))
	}

	return true
}

func loadBloomFilterFromFile(filename string) (*strukture.BloomFilter, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	data := make([]byte, fileSize)

	_, err = file.Read(data)
	if err != nil {
		return nil, err
	}

	bloomFilter, err := strukture.DeserializeBloomFilter(data)
	if err != nil {
		return nil, err
	}

	return bloomFilter, nil
}

func GetCurrentHighestFileIndex() (int64, error) {
	var maxIndex int64 = -1

	fileTypes := []string{"data", "filter", "index", "summary", "metadata"}

	for _, fileType := range fileTypes {
		err := os.MkdirAll("data/"+fileType, os.ModePerm)
		if err != nil {
			return 0, err
		}
	}

	for _, fileType := range fileTypes {
		files, _ := os.ReadDir("data/" + fileType)

		for _, file := range files {
			fileName := file.Name()
			indexInName := strings.Split(fileName, fileType+"_")[1]
			indexInName = strings.Split(indexInName, ".bin")[0]

			index, _ := strconv.ParseInt(string(indexInName), 10, 64)

			maxIndex = max(index, maxIndex)
		}
	}

	if maxIndex == -1 {
		return -1, errors.New("No files found")
	}
	return maxIndex, nil
}

func findFileOffset(filename string, searchKey string, initialOffset int64) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	br := bufio.NewReader(file)

	initialOffsetBytes := make([]byte, initialOffset)
	_, err = io.ReadFull(br, initialOffsetBytes)
	if err != nil {
		return -1, nil
	}

	var previousKey string
	var previousOffset int64

	for {
		// Citaj za duzinu kljuca
		keyLengthBytes := make([]byte, 8)
		_, err := io.ReadFull(br, keyLengthBytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		keyLength := binary.BigEndian.Uint64(keyLengthBytes)

		// Iscitaj kljuc
		keyBytes := make([]byte, keyLength)
		_, err = io.ReadFull(br, keyBytes)
		if err != nil {
			return 0, err
		}
		key := string(keyBytes)

		// Iscitaj sledecih 8 za offset
		offsetBytes := make([]byte, 8)
		_, err = io.ReadFull(br, offsetBytes)
		if err != nil {
			return 0, err
		}
		currentOffset := int64(binary.BigEndian.Uint64(offsetBytes))

		// Da li je key matching
		if key == searchKey {
			return currentOffset, nil
		}

		// Ako nije, nego je pretrazeni veci od onog kojeg zelimo, onda vracamo offset proslog
		if searchKey < key {
			if previousKey != "" {
				return previousOffset, nil
			}
		}

		previousKey = key
		previousOffset = currentOffset
	}

	if previousKey != "" {
		return previousOffset, nil
	}

	return -1, fmt.Errorf("key %s not found", searchKey)
}

func ReadDataFile(fileIndex, dataFileOffset int64, searchKey string) ([]byte, int64, error) {
	file, err := os.Open("data/data/data_" + fmt.Sprint(fileIndex) + ".bin")
	if err != nil {
		return nil, 0, errors.New("Error opening data file")
	}
	defer file.Close()

	br := bufio.NewReader(file)

	initialOffsetBytes := make([]byte, dataFileOffset+12)
	_, err = io.ReadFull(br, initialOffsetBytes)
	if err != nil {
		return nil, 0, errors.New("Error opening data file")
	}

	// Citaj tombstone
	readTombstoneBytes := make([]byte, 1)
	_, err = io.ReadFull(br, readTombstoneBytes)
	if err != nil {
		return nil, 0, errors.New("Error opening data file")
	}
	tombstone := readTombstoneBytes[0]

	// Citaj key size
	readKeySizeBytes := make([]byte, 8)
	_, err = io.ReadFull(br, readKeySizeBytes)
	if err != nil {
		return nil, 0, errors.New("Error opening data file")
	}
	keySize := binary.BigEndian.Uint64(readKeySizeBytes)

	// Citaj value size
	readValueSizeBytes := make([]byte, 8)
	_, err = io.ReadFull(br, readValueSizeBytes)
	if err != nil {
		return nil, 0, errors.New("Error opening data file")
	}
	valueSize := binary.BigEndian.Uint64(readValueSizeBytes)

	// Citaj key
	readKeyBytes := make([]byte, keySize)
	_, err = io.ReadFull(br, readKeyBytes)
	if err != nil {
		return nil, 0, errors.New("Error opening data file")
	}
	key := string(readKeyBytes)

	// Citaj value
	value := make([]byte, valueSize)
	_, err = io.ReadFull(br, value)
	if err != nil {
		return nil, 0, errors.New("Error opening data file")
	}

	if key == searchKey && tombstone == 0 {
		return value, 29 + int64(keySize) + int64(valueSize), nil
	}

	return nil, 29 + int64(keySize) + int64(valueSize), nil
}
