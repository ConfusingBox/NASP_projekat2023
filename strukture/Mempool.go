package strukture

import "errors"

type Mempool struct {
	memtableCount       int64
	activeMemtableIndex int64
	memtables           []*Memtable
}

func CreateMempool(memtableCount, size, structureUsed, skipListDepth, bTreeDegree int64, threshold float64) *Mempool {
	memtables := make([]*Memtable, memtableCount)

	var i int64
	for i = 0; i < memtableCount; i++ {
		memtables[i] = CreateMemtable(size, structureUsed, skipListDepth, bTreeDegree, threshold)
	}

	return &Mempool{memtableCount, 0, memtables}
}

func (mempool *Mempool) Insert(entry *Entry, bloomFilterExpectedElements, indexDensity, summaryDensity, skipListDepth, bTreeDegree int64, bloomFilterFalsePositiveRate float64) error {
	success := mempool.memtables[mempool.activeMemtableIndex].Insert(entry)
	if !success {
		return errors.New("Mempool insert failed")
	}

	if mempool.memtables[mempool.activeMemtableIndex].IsFull() {
		if mempool.activeMemtableIndex == mempool.memtableCount-1 {
			mempool.Flush(bloomFilterExpectedElements, indexDensity, summaryDensity, skipListDepth, bTreeDegree, bloomFilterFalsePositiveRate)
		} else {
			mempool.activeMemtableIndex++
		}
	}

	return nil
}

func (mempool *Mempool) Flush(bloomFilterExpectedElements, indexDensity, summaryDensity, skipListDepth, bTreeDegree int64, bloomFilterFalsePositiveRate float64) error {
	var i int64
	for i = 0; i < mempool.memtableCount; i++ {
		mempool.memtables[i].Flush(bloomFilterExpectedElements, indexDensity, summaryDensity, bloomFilterFalsePositiveRate)
	}

	for i = 0; i < mempool.memtableCount; i++ {
		mempool.memtables[i].Empty(skipListDepth, bTreeDegree)
	}

	mempool.activeMemtableIndex = 0

	return nil
}

// STEFANE URADI OVO
func (mp *Mempool) Find(key string) *Entry {

	for i := mp.activeMemtableIndex; i >= 0; i-- {
		entry := mp.memtables[i].Find(key)
		if entry != nil {
			return entry
		}
	}

	return nil
}

/*
func (mp *Mempool) Exists(key []byte) (bool, int) {
	for i := 0; i < mp.tableCount; i++ {
		tableIdx := (mp.activeTableIdx - i + mp.tableCount) % mp.tableCount // the addition makes sure we dont get negative numbers
		if mp.tables[tableIdx].Exists(key) {
			return true, tableIdx
		}
	}
	return false, -1
}


func (mp *Mempool) IsFull() bool {
	for i := 0; i < mp.tableCount; i++ {
		if !mp.tables[i].IsFull() {
			return false
		}
	}
	return true
}

func (mp *Mempool) Get(key []byte, tableIdx int) (*MemtableEntry, error) {
	return mp.tables[tableIdx].Get(key)
}

func (mp *Mempool) Put(entry *MemtableEntry) error {
	exists := mp.tables[mp.activeTableIdx].Exists(entry.Key)
	err := error(nil)
	if exists {
		err = mp.tables[mp.activeTableIdx].Delete(entry.Key)
		if err != nil {
			return err
		}
	}
	err = mp.tables[mp.activeTableIdx].Insert(entry)
	if err != nil {
		return err
	}
	if mp.tables[mp.activeTableIdx].IsFull() {
		nextIdx := (mp.activeTableIdx + 1) % mp.tableCount
		if mp.IsFull() {
			err = mp.tables[nextIdx].Flush(0, 0, 0, 0, 0.0, false)
			return err
		}
		mp.activeTableIdx = nextIdx
	}
	return err
}
*/
