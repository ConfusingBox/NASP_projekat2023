package main

import (
	"NASP_projekat2023/strukture"
	"NASP_projekat2023/utils"
	"fmt"
	"os"
	"strings"
)

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
			serializedBf := strukture.SerializeBloomFilter(bf)
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
			// SimHash dodatak
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
	strukture.TestBTree()
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

			if engine.Get(key) {
				fmt.Print("Get operation successful.")
			} else {
				fmt.Print("Get operation failed.")
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
			// probabilisticStructs(config)
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
