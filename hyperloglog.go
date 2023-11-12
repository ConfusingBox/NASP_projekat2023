package proj

import (
	"bytes"
	"encoding/gob"
	"math"
	"math/bits"
)

type HyperLogLog struct {
	Precision int
	Registers []int
}

func NewHyperLogLog(precision int) *HyperLogLog {
	size := 1 << precision //Shiftujemo size u levo za odredjeni precision, kako bi zauzeli 'precision' mesta u bucketima.
	return &HyperLogLog{precision, make([]int, size)}
}

func Delete(hll *HyperLogLog) {
	for i := range hll.Registers {
		hll.Registers[i] = 0
	}
}

func (HLL *HyperLogLog) SerializeHLL() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(HLL)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func DeserializeHLL(data []byte) (*BloomFilter, error) {
	var HLL BloomFilter
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	err := decoder.Decode(&HLL)
	if err != nil {
		return nil, err
	}
	return &HLL, nil
}

func (hll *HyperLogLog) Add(item string) {
	h1 := bloomflh1(item, 1<<hll.Precision)
	h2 := bloomflh2(item, 1<<hll.Precision)
	h3 := bloomflh3(item, 1<<hll.Precision)
	h4 := bloomflh4(item, 1<<hll.Precision)

	hashValue := uint64(h1 | h2 | h3 | h4)

	index := hashValue & ((1 << hll.Precision) - 1)
	leadingZeros := 0
	for i := uint(63); i >= 0; i-- {
		if (hashValue & (1 << i)) == 0 {
			leadingZeros++
		} else {
			break
		}
	}
	hll.Registers[index] = int(math.Max(float64(hll.Registers[index]), float64(leadingZeros+1)))
}

func leadingZeroCount(n uint64) int {
	return bits.LeadingZeros64(n) + 1
}
