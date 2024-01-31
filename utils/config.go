package utils

<<<<<<< Updated upstream
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
	BloomFilterFalsePositiveRate float32 `json:"bloom_filter_false_positive_rate"`
	BloomFilterExpectedElements  int     `json:"bloom_filter_expected_elements"`
	SkipListDepth                int     `json:"skip_list_depth"`
	HyperLogLogPrecision         int     `json:"hyperloglog_precision"`
	WALDirectory                 string  `json:"wal_directory"`
	WALBufferSize                int     `json:"wal_buffer_size"`
	WALSegmentSize               int     `json:"wal_segment_size"`
	BTreeDegree                  int     `json:"btree_degree"`
	MemTableSize                 int     `json:"mem_table_size"`
	MemTableType                 string  `json:"mem_table_type"`
	MemPoolSize                  int     `json:"mem_pool_size"`
	SummaryDensity               int     `json:"summary_density"`
	IndexDensity                 int     `json:"index_density"`
	SSTableMultipleFiles         bool    `json:"ss_table_multiple_files"`
	SSTableDirectory             string  `json:"ss_table_directory"`
	CacheSize                    int     `json:"cache_size"`
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
	MemTableSize:                 10000,
	MemTableType:                 "skip_list",
	MemPoolSize:                  10,
	SummaryDensity:               5,
	IndexDensity:                 5,
	SSTableMultipleFiles:         true,
	SSTableDirectory:             "data/sstable",
	CacheSize:                    20,
}

func LoadConfig(filepath string) (*Config, error) {
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
	if config.MemTableSize <= 0 {
		config.MemTableSize = DefaultConfig.MemTableSize
	}
	if config.MemTableType == "" {
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

	return &config, err
}
=======
type Config struct {
	bloomFilterFalsePositiveRate float32 `json: "bloom_filter_false_positive_rate"`
	bloomFilterExpectedElemets   uint32  `json: "bloom_filter_expected_elements"`
	skipListDepth                uint32  `json: "skip_list_depth"`
	hyperLogLogPrecision         uint32  `json: "hyperloglog_precision`
}

/*
bloom_filter_probability: 0.1,
bloom_filter_cap: 1_000_000,
skip_list_max_level: 10,
hyperloglog_precision: 10,
write_ahead_log_dir: "./wal/".to_string(),
write_ahead_log_num_of_logs: 1000,
write_ahead_log_size: 1048576,
b_tree_order: 10,
memory_table_capacity: 1000,
memory_table_type: MemoryTableType::SkipList,
memory_table_pool_num: 10,
summary_density: 3,
index_density: 2,
sstable_single_file: false,
sstable_dir: "./sstables/".to_string(),
lsm_max_level: 0,
lsm_max_per_level: 0,
compaction_enabled: false,
compaction_algorithm_type: CompactionAlgorithmType::SizeTiered,
cache_max_size: 0,
token_bucket_num: 0,
token_bucket_interval: 0,
use_compression: false,
compression_dictionary_path: "./dictionary.bin".to_string(),
*/
>>>>>>> Stashed changes
