package proj

import (
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
		BitArray: make([]byte, size),
		NumHash:  numHash,
	}
}

func (bf *BloomFilter) Delete() {
	bf.BitArray = make([]byte, len(bf.BitArray))
}

func lookup(bitarray []byte, arrSize int, s string) bool {
	a := bloomflh1(s, arrSize)
	b := bloomflh2(s, arrSize)
	c := bloomflh3(s, arrSize)
	d := bloomflh4(s, arrSize)

	if bitarray[a]>>7&1 == 1 && bitarray[b]>>7&1 == 1 && bitarray[c]>>7&1 == 1 && bitarray[d]>>7&1 == 1 {
		return true
	}
	return false
}

func insert(bitarray []byte, arrSize int, s string) {
	a := bloomflh1(s, arrSize)
	b := bloomflh2(s, arrSize)
	c := bloomflh3(s, arrSize)
	d := bloomflh4(s, arrSize)

	bitarray[a] |= 1 << 7
	bitarray[b] |= 1 << 7
	bitarray[c] |= 1 << 7
	bitarray[d] |= 1 << 7
	//Kazu kao da |= za razliku od = se koristi kad menjamo samo jedan bit bez druge da ometamo tako nesto
	// 1 << 7 -> 1 (vrednost koju zelimo da postavimo) << <= (operator za pomeranje ulevo) 7 <= (za 7 pozicija) ==> znaci da se u bajtu 0b00000000 pomeramo 7 mesta ulevo i menjamo taj bit na vrednost 1
}
