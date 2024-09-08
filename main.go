package main

import (
	"NASP_projekat2023/strukture"
	"NASP_projekat2023/utils"
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

// func generateUniqueEntries(count int) map[string][]byte {
// 	entries := make(map[string][]byte)
// 	for i := 0; i < count; i++ {
// 		key := fmt.Sprintf("key_%d", i)
// 		value := fmt.Sprintf("value_%d", i)
// 		entries[key] = []byte(value)
// 	}
// 	return entries
// }
// func putManyEntries(engine *Engine, count int) {
// 	entries := generateUniqueEntries(count)
// 	for key, value := range entries {
// 		if engine.Put(key, value) {
// 			fmt.Printf("Put operation successful for key: %s\n", key)
// 		} else {
// 			fmt.Printf("Put operation failed for key: %s\n", key)
// 		}
// 	}
// }

func probabilisticStructs(config *utils.Config) {
	bf := strukture.NewBloomFilterWithSize(config.BloomFilterExpectedElements, config.BloomFilterFalsePositiveRate)
	//cms := strukture.NewCountMinSketch(config.MemTableSize, config.SkipListDepth)
	hll := strukture.NewHyperLogLog(config.HyperLogLogPrecision)

	for {
		fmt.Println("Probabilistic Structures Menu:")
		fmt.Println("1. Bloom Filter")
		fmt.Println("2. Count-Min Sketch")
		fmt.Println("3. HyperLogLog")
		fmt.Println("4. SimHash")
		fmt.Println("x. Back")

		var choice string
		fmt.Print("Enter your choice: ")
		fmt.Scan(&choice)

		switch strings.ToLower(choice) {
		case "1":
			strukture.BloomFilterMenu(bf)
			serializedBf := bf.Serialize()
			fmt.Println(serializedBf)
		case "2":
			//strukture.CMSMenu(cms)
			//serializedCMS := cms.SerializeCMS()
			//fmt.Println(serializedCMS)
		case "3":
			strukture.HLLMenu(hll)
			serializedHLL := hll.SerializeHLL()
			fmt.Println(serializedHLL)
		case "4":
		case "x":
			return
		default:
			fmt.Println("Pogrešan izbor. Pokušajte opet.")
		}
	}
}

func main() {
	engine := Engine{}
	engine.LoadStructures()
	//strukture.WriteAheadLogTest()

	err := processWALFile(engine.Mempool, "data/wal/wal_0.bin", engine.Config.BloomFilterExpectedElements, engine.Config.IndexDensity, engine.Config.SummaryDensity, engine.Config.SkipListDepth, engine.Config.BTreeDegree, engine.Config.BloomFilterFalsePositiveRate)
	if err != nil {
		fmt.Printf("Error processing WAL file: %v\n", err)
	}
	for {
		fmt.Println("Main Menu:")
		fmt.Println("1. Put")
		fmt.Println("2. Get")
		fmt.Println("3. Delete")
		fmt.Println("4. Koristi probabilisticke strukture")
		fmt.Println("5. ClearLog")
		fmt.Println("x. Izlaz")

		var choice string
		fmt.Print("Enter your choice: ")
		fmt.Scan(&choice)

		switch strings.ToLower(choice) {
		case "1":
			// Put operation
			var key, value string

			fmt.Print("Enter key: ")
			fmt.Scan(&key)
			fmt.Print("Enter value: ")
			fmt.Scan(&value)

			if engine.Put(key, []byte(value)) {
				fmt.Print("Put operation successful.")
			} else {
				fmt.Print("Put operation failed.")
			}
		case "2":
			// Get operation
			var key string

			fmt.Print("Enter key: ")
			fmt.Scan(&key)

			value, ok := engine.Get(key)
			if ok {
				fmt.Printf("Get operation successful. Value: %s\n", string(value))
			} else {
				fmt.Println("Get operation failed. Key not found.")
			}
		case "3":
			// Delete operation
			var key string

			fmt.Print("Enter key: ")
			fmt.Scan(&key)

			if engine.Delete(key) {
				fmt.Print("Delete operation successful.")
			} else {
				fmt.Print("Delete operation failed.")
			}
		case "4":
			probabilisticStructs(engine.Config)

		case "5":
			// Clear Log
		case "x":
			fmt.Println("Izlaz")
			os.Exit(0)
		default:
			fmt.Println("Pogrešan izbor. Pokušajte opet.")
		}
	}
}

func processWALFile(mempool *strukture.Mempool, filePath string, bloomFilterExpectedElements, indexDensity, summaryDensity, skipListDepth, bTreeDegree int64, bloomFilterFalsePositiveRate float64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	br := bufio.NewReader(file)

	for {
		entry, err := readEntryFromWAL(br)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := mempool.Insert(entry, bloomFilterExpectedElements, indexDensity, summaryDensity, skipListDepth, bTreeDegree, bloomFilterFalsePositiveRate); err != nil {
			return err
		}
	}

	return nil
}
func readEntryFromWAL(br *bufio.Reader) (*strukture.Entry, error) {
	crcBytes := make([]byte, 4)
	if _, err := io.ReadFull(br, crcBytes); err != nil {
		return nil, err
	}

	timestampBytes := make([]byte, 8)
	if _, err := io.ReadFull(br, timestampBytes); err != nil {
		return nil, err
	}

	tombstoneByte := make([]byte, 1)
	if _, err := io.ReadFull(br, tombstoneByte); err != nil {
		return nil, err
	}
	tombstone := tombstoneByte[0]

	keySizeBytes := make([]byte, 8)
	if _, err := io.ReadFull(br, keySizeBytes); err != nil {
		return nil, err
	}
	keySize := binary.BigEndian.Uint64(keySizeBytes)

	fmt.Printf("Read key size: %d\n", keySize)

	if keySize > 1<<20 { // Example limit
		return nil, fmt.Errorf("key size too large: %d", keySize)
	}
	keyBytes := make([]byte, keySize)
	if _, err := io.ReadFull(br, keyBytes); err != nil {
		return nil, err
	}
	key := string(keyBytes)

	valueSizeBytes := make([]byte, 8)
	if _, err := io.ReadFull(br, valueSizeBytes); err != nil {
		return nil, err
	}
	valueSize := binary.BigEndian.Uint64(valueSizeBytes)

	if valueSize > 1<<20 { // Example limit
		return nil, fmt.Errorf("value size too large: %d", valueSize)
	}
	valueBytes := make([]byte, valueSize)
	if _, err := io.ReadFull(br, valueBytes); err != nil {
		return nil, err
	}
	value := valueBytes

	entry := strukture.CreateEntry(key, value, tombstone)
	fmt.Println(entry)
	return entry, nil
}
