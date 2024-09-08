package main

import (
	"NASP_projekat2023/strukture"
	"NASP_projekat2023/utils"
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
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

func (engine *Engine) Get(key string) ([]byte, bool) {
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

	bloomFilter, err := loadBloomFilterFromFile("data/filter_0.bin")
	if err != nil {
		fmt.Println("Error loading Bloom Filter:", err)
		return nil, false
	}

	if !bloomFilter.Lookup(key) {
		fmt.Println("Key not found in Bloom Filter")
		return nil, false
	}

	offset, err := findOffsetInSummaryFile("data/summary_0.bin", key)
	if err != nil {
		fmt.Println("Error finding offset:", err)
		return nil, false
	}
	fmt.Println(offset)
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

func findOffsetInSummaryFile(filename string, searchKey string) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	br := bufio.NewReader(file)

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

	return 0, fmt.Errorf("key %s not found", searchKey)
}
