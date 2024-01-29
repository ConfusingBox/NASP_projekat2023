package strukture

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"math"

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

func (bloomfilter *BloomFilter) SerializeBF() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(bloomfilter)
	if err != nil {
		return nil, err
	}

	//--- big endian ---
	result := make([]byte, buffer.Len())
	binary.BigEndian.PutUint64(result, uint64(buffer.Len()))
	copy(result[8:], buffer.Bytes())
	//--- big endian ---
	return result, nil
}

func DeserializeBF(data []byte) (*BloomFilter, error) {
	var bloomfilter BloomFilter
	//--- big endian ---
	length := binary.BigEndian.Uint64(data[:8])
	buffer := bytes.NewBuffer(data[8 : 8+length])

	decoder := gob.NewDecoder(buffer)

	err := decoder.Decode(&bloomfilter)
	if err != nil {
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
