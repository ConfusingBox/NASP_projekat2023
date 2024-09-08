package main

import (
	"NASP_projekat2023/strukture"
	"NASP_projekat2023/utils"
	"fmt"
)

type Engine struct {
	Config      *utils.Config
	TokenBucket *strukture.TokenBucket
	WAL         *strukture.WriteAheadLog
	Cache       *strukture.LRUCache
	MemTable    *strukture.Memtable
	BloomFilter *strukture.BloomFilter
}

func (engine *Engine) LoadStructures() bool {
	Config, err := utils.LoadConfigValues("config.json")
	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	Memtable := strukture.CreateMemtable(Config.MemTableSize, Config.MemTableType, Config.SkipListDepth, Config.BTreeDegree, Config.MemTableThreshold)
	TokenBucket := strukture.NewTokenBucket(int(Config.TokenBucketLimitSeconds), int(Config.TokenBucketCapacity))
	WAL, err1 := strukture.CreateWriteAheadLog(Config.WALSegmentSize)
	if err1 != nil {
		fmt.Println(err1.Error())
		return false
	}
	Cache := strukture.NewLRUCache(Config.CacheSize)

	BloomFilter := strukture.NewBloomFilterWithSize(Config.BloomFilterExpectedElements, Config.BloomFilterFalsePositiveRate)

	*engine = Engine{
		Config:      Config,
		WAL:         WAL,
		TokenBucket: TokenBucket,
		Cache:       &Cache,
		MemTable:    Memtable,
		BloomFilter: BloomFilter,
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

	err = engine.MemTable.Insert(entry)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	engine.BloomFilter.Insert(key)

	return true
}

func (engine *Engine) Get(key string) ([]byte, bool) {
	if !engine.TokenBucket.Allow() {
		fmt.Println("Wait until request is available")
		return nil, false
	}

	if !engine.BloomFilter.Lookup(key) {
		fmt.Println("Key not found in Bloom Filter")
		return nil, false
	}

	if value := engine.Cache.Get([]byte(key)); value != nil {
		return value, true
	}

	if entry := engine.MemTable.Get(key); entry != nil {
		return entry.GetValue(), true
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

	err = engine.MemTable.Insert(entry)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	// Remove the key from the Cache if it exists
	if value := engine.Cache.Get([]byte(key)); value != nil {
		engine.Cache.Remove([]byte(key))
	}

	// Optionally: Update the Bloom Filter to indicate that the key might be deleted
	engine.BloomFilter.Insert(key) // This might be used to indicate the key was deleted

	return true
}
