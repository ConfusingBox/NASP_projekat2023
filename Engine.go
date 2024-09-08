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
}

func (engine *Engine) LoadStructures() bool {
	// inicijalizovati sve strukture
	// ne brini o tome kako se to radi, samo im pozovi konstruktor, on ce se pobrinuti da se kreiraju ispravno

	Config, err := utils.LoadConfigValues("config.json")
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	Memtable, err2 := strukture.NewMemtable(int(Config.MemTableSize), int(Config.SkipListDepth), int(Config.BTreeDegree), Config.MemTableType)
	if err2 != nil {
		fmt.Println(err2.Error())
		return false
	}

	TokenBucket := strukture.NewTokenBucket(int(Config.TokenBucketLimitSeconds), int(Config.TokenBucketCapacity))
	WAL, err1 := strukture.CreateWriteAheadLog(Config.WALSegmentSize)
	if err1 != nil {
		fmt.Println(err1.Error())
		return false
	}
	Cache := strukture.NewLRUCache(Config.CacheSize)

	engine = &Engine{Config: Config, TokenBucket: TokenBucket, WAL: WAL, Cache: &Cache, MemTable: Memtable}

	return true
}

func (engine *Engine) Put(key string, value []byte) bool {
	/*
		1.1. Upisi u write ahead log
		1.2. Ako je uspjesno idi dalje
		2. Upisi u memtable
	*/

	entry := strukture.CreateEntry(key, value, 0)
	err := engine.WAL.Log(entry)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	// if nema greske

	//engine.MemTable.Insert(entry)

	return true
}

func (engine *Engine) Get(key string) bool {
	return true
}

func (engine *Engine) Delete(key string) bool {
	return true
}
