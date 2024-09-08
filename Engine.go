package main

type Engine struct {
	// dodati po jednu od svake strukture
}

func (engine *Engine) LoadStructures() bool {
	// inicijalizovati sve strukture
	// ne brini o tome kako se to radi, samo im pozovi konstruktor, on ce se pobrinuti da se kreiraju ispravno

	return true
}

func (engine *Engine) Put(key string, value []byte) bool {
	/*
		1.1. Upisi u write ahead log
		1.2. Ako je uspjesno idi dalje
		2. Upisi u memtable
	*/

	// entry := CreateEntry(key, value, false)
	// engine.writeAheadLog.Log(entry)
	// if nema greske
	// engine.memTable.Write(entry)

	return true
}

func (engine *Engine) Get(key string) bool {
	return true
}

func (engine *Engine) Delete(key string) bool {
	return true
}
