package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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

func LoadConfigValues(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}

	return &config, nil
}
