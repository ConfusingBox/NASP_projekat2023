package strukture

import (
	"bytes"
	"encoding/binary"
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

func SerializeBloomFilter(bf *BloomFilter) []byte {

	sizeByteHash := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeByteHash, uint64(bf.NumHash))

	returnArray := append(sizeByteHash, bf.BitArray...)

	return returnArray
}

func DeserializeBloomFilter(data []byte) (*BloomFilter, error) {
	reader := bytes.NewReader(data[8:])
	numHashBytes := make([]byte, 8)
	copy(numHashBytes, data[:8])

	bitArray := make([]byte, len(data)-8)
	err := binary.Read(reader, binary.BigEndian, &bitArray)
	if err != nil {
		return nil, err
	}
	numHash := binary.BigEndian.Uint64(numHashBytes)

	return &BloomFilter{
		NumHash:  int(numHash),
		BitArray: bitArray,
	}, nil
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
