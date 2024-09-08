package utils

import (
	"encoding/json"
	"os"
)

const (
	KB = 1 << (10 * iota) // 1 kilobyte
	MB                    // 1 megabyte
	GB                    // 1 gigabyte
	TB                    // 1 terabyte
)

type Config struct {
	BloomFilterFalsePositiveRate float64 `json:"bloom_filter_false_positive_rate"`
	BloomFilterExpectedElements  int64   `json:"bloom_filter_expected_elements"`
	SkipListDepth                int64   `json:"skip_list_depth"`
	HyperLogLogPrecision         int64   `json:"hyperloglog_precision"`
	WALDirectory                 string  `json:"wal_directory"`
	WALBufferSize                int64   `json:"wal_buffer_size"`
	WALSegmentSize               int64   `json:"wal_segment_size"`
	BTreeDegree                  int64   `json:"btree_degree"`
	MemTableThreshold            float64 `json:"mem_table_threshold"`
	MemTableSize                 int64   `json:"mem_table_size"`
	MemTableType                 int64   `json:"mem_table_type"`
	MemPoolSize                  int64   `json:"mem_pool_size"`
	SummaryDensity               int64   `json:"summary_density"`
	IndexDensity                 int64   `json:"index_density"`
	SSTableMultipleFiles         bool    `json:"ss_table_multiple_files"`
	SSTableDirectory             string  `json:"ss_table_directory"`
	CacheSize                    int64   `json:"cache_size"`
	TokenBucketCapacity          int64   `json:"token_bucket_capacity"`
	TokenBucketLimitSeconds      int64   `json:"token_bucket_limit_seconds"`
	SimHashHashSize              int64   `json:"sim_hash_hash_size"`
}

var DefaultConfig = Config{
	BloomFilterFalsePositiveRate: 0.2,
	BloomFilterExpectedElements:  50000,
	SkipListDepth:                10,
	HyperLogLogPrecision:         10,
	WALDirectory:                 "data/log",
	WALBufferSize:                100,
	WALSegmentSize:               1 * MB,
	BTreeDegree:                  10,
	MemTableThreshold:            70.0,
	MemTableSize:                 10000,
	MemTableType:                 1,
	MemPoolSize:                  10,
	SummaryDensity:               5,
	IndexDensity:                 5,
	SSTableMultipleFiles:         true,
	SSTableDirectory:             "data/sstable",
	CacheSize:                    20,
	SimHashHashSize:              8,
}

func LoadConfigValues(filepath string) (*Config, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	var config Config
	err = json.Unmarshal(data, &config)

	if err != nil {
		return nil, err
	}

	if config.BloomFilterFalsePositiveRate <= 0 || config.BloomFilterFalsePositiveRate > 1 {
		config.BloomFilterFalsePositiveRate = DefaultConfig.BloomFilterFalsePositiveRate
	}
	if config.BloomFilterExpectedElements <= 0 {
		config.BloomFilterExpectedElements = DefaultConfig.BloomFilterExpectedElements
	}
	if config.SkipListDepth <= 0 {
		config.SkipListDepth = DefaultConfig.SkipListDepth
	}
	if config.HyperLogLogPrecision <= 0 {
		config.HyperLogLogPrecision = DefaultConfig.HyperLogLogPrecision
	}
	if config.WALDirectory == "" {
		config.WALDirectory = DefaultConfig.WALDirectory
	}
	if config.WALBufferSize <= 0 {
		config.WALBufferSize = DefaultConfig.WALBufferSize
	}
	if config.WALSegmentSize <= 0 {
		config.WALSegmentSize = DefaultConfig.WALSegmentSize
	}
	if config.BTreeDegree <= 0 {
		config.BTreeDegree = DefaultConfig.BTreeDegree
	}
	if config.MemTableThreshold <= 0 || config.MemTableThreshold >= 100 {
		config.MemTableThreshold = DefaultConfig.MemTableThreshold
	}
	if config.MemTableSize <= 0 {
		config.MemTableSize = DefaultConfig.MemTableSize
	}
	if config.MemTableType < 1 {
		config.MemTableType = DefaultConfig.MemTableType
	}
	if config.MemTableType > 3 {
		config.MemTableType = DefaultConfig.MemTableType
	}
	if config.MemPoolSize <= 0 {
		config.MemPoolSize = DefaultConfig.MemPoolSize
	}
	if config.SummaryDensity <= 0 {
		config.SummaryDensity = DefaultConfig.SummaryDensity
	}
	if config.IndexDensity <= 0 {
		config.IndexDensity = DefaultConfig.IndexDensity
	}
	if config.SSTableDirectory == "" {
		config.SSTableDirectory = DefaultConfig.SSTableDirectory
	}
	if config.CacheSize <= 0 {
		config.CacheSize = DefaultConfig.CacheSize
	}
	if config.SimHashHashSize <= 0 {
		config.CacheSize = DefaultConfig.CacheSize
	}

	return &config, err
}
