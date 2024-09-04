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

func main() {
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	bf := strukture.NewBloomFilterWithSize(config.BloomFilterExpectedElements, config.BloomFilterFalsePositiveRate)

	cms := strukture.NewCountMinSketch(config.MemTableSize, config.SkipListDepth)

	hll := strukture.NewHyperLogLog(config.HyperLogLogPrecision)

	/*
		data := "NestoNesto"
		sh, err := strukture.NewSimHashWithFingerprint(data, config.SimHashHashSize)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
	*/

	for {
		fmt.Println("Main Menu:")
		fmt.Println("1. Bloom Filter")
		fmt.Println("2. Count-Min Sketch")
		fmt.Println("3. HyperLogLog")
		fmt.Println("4. SimHash")
		fmt.Println("x. Exit")

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
			/*
				data := "NestoNesto"
				sh, err := strukture.NewSimHashWithFingerprint(data, config.SimHashHashSize)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				strukture.SimHashMenu(sh)
			*/
		case "x":
			os.Exit(0)
		default:
			fmt.Println("Invalid choice. Please try again.")
		}
	}
}
