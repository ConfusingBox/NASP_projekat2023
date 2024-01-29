package strukture

import (
	"encoding/gob"
	"math"
	"os"

	hashfunc "NASP_projekat2023/utils"
)

type BloomFilter struct {
	BitArray []byte
	NumHash  int
}

func NewBloomFilterWithSize(expectedElements int, falsePositiveRate float64) *BloomFilter {
	size := calculateSize(expectedElements, falsePositiveRate)
	numHash := calculateNumHash(expectedElements, size)
	return &BloomFilter{
		BitArray: make([]byte, size),
		NumHash:  numHash,
	}
}

func (bloomfilter *BloomFilter) SerializeBF(filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(bloomfilter); err != nil {
		return err
	}

	return nil
}

func DeserializeBFFromFile(filepath string) (*BloomFilter, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var bloomfilter BloomFilter
	decoder := gob.NewDecoder(file)

	if err := decoder.Decode(&bloomfilter); err != nil {
		return nil, err
	}

	return &bloomfilter, nil
}

func (bf *BloomFilter) Delete() {
	bf.BitArray = make([]byte, len(bf.BitArray))
}

func (bf *BloomFilter) Lookup(s string) bool {
	for i := 0; i < bf.NumHash; i++ {
		index := hashfunc.CustomHash(s, len(bf.BitArray), i)
		if bf.BitArray[index]>>7&1 != 1 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Insert(s string) {
	for i := 0; i < bf.NumHash; i++ {
		index := hashfunc.CustomHash(s, len(bf.BitArray), i)
		bf.BitArray[index] |= 1 << 7
	}
}

func calculateSize(expectedElements int, falsePositiveRate float64) int {
	return int(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func calculateNumHash(expectedElements, size int) int {
	return int(math.Ceil((float64(size) / float64(expectedElements)) * math.Log(2)))
}
