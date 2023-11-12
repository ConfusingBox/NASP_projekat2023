package proj

import (
	"bytes"
	_ "bytes"
	"encoding/gob"
	_ "encoding/gob"
	_ "fmt"
)

type BloomFilter struct {
	BitArray []bool
	NumHash  int
}

func (bf *BloomFilter) SerializeBF() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(bf)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func DeserializeBF(data []byte) (*BloomFilter, error) {
	var bf BloomFilter
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	err := decoder.Decode(&bf)
	if err != nil {
		return nil, err
	}
	return &bf, nil
}

func NewBloomFilter(size int, numHash int) *BloomFilter {
	return &BloomFilter{
		BitArray: make([]bool, size),
		NumHash:  numHash,
	}
}

func (bf *BloomFilter) Delete() {
	bf.BitArray = make([]bool, len(bf.BitArray))
}

func lookup(bitarray []bool, arrSize int, s string) bool {
	a := bloomflh1(s, arrSize)
	b := bloomflh2(s, arrSize)
	c := bloomflh3(s, arrSize)
	d := bloomflh4(s, arrSize)

	if bitarray[a] && bitarray[b] && bitarray[c] && bitarray[d] {
		return true
	}
	return false
}

func insert(bitarray []bool, arrSize int, s string) {
	a := bloomflh1(s, arrSize)
	b := bloomflh2(s, arrSize)
	c := bloomflh3(s, arrSize)
	d := bloomflh4(s, arrSize)

	bitarray[a] = true
	bitarray[b] = true
	bitarray[c] = true
	bitarray[d] = true
}
