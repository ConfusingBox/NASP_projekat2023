package strukture

import (
	hashfunc "NASP_projekat2023/utils"
	"bytes"
	_ "bytes"
	"encoding/gob"
	_ "encoding/gob"
	_ "fmt"
)

type BloomFilter struct {
	BitArray []byte
	NumHash  int
}

func (bloomfilter *BloomFilter) SerializeBF() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(bloomfilter)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func DeserializeBF(data []byte) (*BloomFilter, error) {
	var bloomfilter BloomFilter
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	err := decoder.Decode(&bloomfilter)
	if err != nil {
		return nil, err
	}
	return &bloomfilter, nil
}

func NewBloomFilter(size int, numHash int) *BloomFilter {
	return &BloomFilter{
		BitArray: make([]byte, size),
		NumHash:  numHash,
	}
}

func (bf *BloomFilter) Delete() {
	bf.BitArray = make([]byte, len(bf.BitArray))
}

func lookup(bitarray []byte, arrSize int, s string) bool {
	index1 := hashfunc.Hash1(s, arrSize)
	index2 := hashfunc.Hash2(s, arrSize)
	index3 := hashfunc.Hash3(s, arrSize)
	index4 := hashfunc.Hash4(s, arrSize)

	if bitarray[index1]>>7&1 == 1 && bitarray[index2]>>7&1 == 1 && bitarray[index3]>>7&1 == 1 && bitarray[index4]>>7&1 == 1 {
		return true
	}
	return false
}

func insert(bitarray []byte, arrSize int, s string) {
	index1 := hashfunc.Hash1(s, arrSize)
	index2 := hashfunc.Hash2(s, arrSize)
	index3 := hashfunc.Hash3(s, arrSize)
	index4 := hashfunc.Hash4(s, arrSize)

	bitarray[index1] |= 1 << 7
	bitarray[index2] |= 1 << 7
	bitarray[index3] |= 1 << 7
	bitarray[index4] |= 1 << 7
	//Kazu kao da |= za razliku od = se koristi kad menjamo samo jedan bit bez druge da ometamo tako nesto
	// 1 << 7 -> 1 (vrednost koju zelimo da postavimo) << <= (operator za pomeranje ulevo) 7 <= (za 7 pozicija) ==> znaci da se u bajtu 0b00000000 pomeramo 7 mesta ulevo i menjamo taj bit na vrednost 1
}
