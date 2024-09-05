package main

import (
	"NASP_projekat2023/strukture"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type Config struct {
	BloomFilterFalsePositiveRate float64 `json:"bloom_filter_false_positive_rate"`
	BloomFilterExpectedElements  int     `json:"bloom_filter_expected_elements"`
	SkipListDepth                int     `json:"skip_list_depth"`
	HyperLogLogPrecision         int     `json:"hyperloglog_precision"`
	WalDirectory                 string  `json:"wal_directory"`
	WalBufferSize                int     `json:"wal_buffer_size"`
	WalSegmentSize               int     `json:"wal_segment_size"`
	BtreeDegree                  int     `json:"btree_degree"`
	MemTableThreshold            int     `json:"mem_table_threshold"`
	MemTableSize                 int     `json:"mem_table_size"`
	MemTableType                 string  `json:"mem_table_type"`
	MemPoolSize                  int     `json:"mem_pool_size"`
	SummaryDensity               int     `json:"summary_density"`
	IndexDensity                 int     `json:"index_density"`
	SsTableMultipleFiles         bool    `json:"ss_table_multiple_files"`
	SsTableDirectory             string  `json:"ss_table_directory"`
	CacheSize                    int     `json:"cache_size"`
	SimHashHashSize              int     `json:"sim_hash_hash_size"`
}

func loadConfig(filename string) (Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}

	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return Config{}, err
	}

	return config, nil
}

func probabilisticStructs(config Config) {
	bf := strukture.NewBloomFilterWithSize(config.BloomFilterExpectedElements, config.BloomFilterFalsePositiveRate)
	cms := strukture.NewCountMinSketch(config.MemTableSize, config.SkipListDepth)
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
		case "2":
			strukture.CMSMenu(cms)
		case "3":
			strukture.HLLMenu(hll)
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
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
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
		case "2":
			// Get operation
		case "3":
			// Delete operation
		case "4":
			probabilisticStructs(config)
		case "5":
			// Ciscenje Log-a
		case "x":
			fmt.Println("Izlaz")
			os.Exit(0)
		default:
			fmt.Println("Pogrešan izbor. Pokušajte opet.")
		}
	}
}
