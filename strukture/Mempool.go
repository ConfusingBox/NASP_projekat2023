package strukture

type Mempool struct {
	memtableCount       int64
	activeMemtableIndex int64
	memtables           []*Memtable
}

/*
func NewMempool(numTables, memtableSize, skipListDepth, BTreeDegree int, outputDir, memtableType string) (*Mempool, error) {
	memtables := make([]*Memtable, numTables)
	var err error
	for i := 0; i < numTables; i++ {
		memtables[i], err = NewMemtable(memtableSize, skipListDepth, BTreeDegree, memtableType)
	}

	return &Mempool{memtableCount, 0, memtables}
}

func (mempool *Mempool) Insert(entry *Entry) error {
	success := mempool.memtables[mempool.activeMemtableIndex].Insert(entry)
	if !success {
		return errors.New("Mempool insert failed.")
	}

	if mempool.memtables[mempool.activeMemtableIndex].IsFull() {
		if mempool.activeMemtableIndex == mempool.memtableCount {
			mempool.Flush()
		} else {
			mempool.activeMemtableIndex++
		}
	}

	return nil
}

func (mempool *Mempool) Flush() error {
	var i int64
	for i = 0; i < mempool.memtableCount; i++ {
		mempool.memtables[i].Flush()
	}

	for i = 0; i < mempool.memtableCount; i++ {
		mempool.memtables[i].Empty()
	}

	mempool.activeMemtableIndex = 0

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

/*
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
