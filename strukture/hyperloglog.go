package strukture

import (
	hashfunc "NASP_projekat2023/utils"
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

func DeserializeHLL(data []byte) (*HyperLogLog, error) {
	var hyperloglog HyperLogLog
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	err := decoder.Decode(&hyperloglog)
	if err != nil {
		return nil, err
	}
	return &hyperloglog, nil
}

// func (hyperloglog *HyperLogLog) Estimate() float64 {
//		dodacu ovo
// }

func (hyperloglog *HyperLogLog) Add(item string) {
	hash1 := hashfunc.Hash1(item, 1<<hyperloglog.Precision)
	hash2 := hashfunc.Hash2(item, 1<<hyperloglog.Precision)
	hash3 := hashfunc.Hash3(item, 1<<hyperloglog.Precision)
	hash4 := hashfunc.Hash4(item, 1<<hyperloglog.Precision)

	hashValue := uint64(hash1 | hash2 | hash3 | hash4)

	index := hashValue & ((1 << hyperloglog.Precision) - 1)
	leadingZeros := 0
	for i := uint(63); i >= 0; i-- {
		if (hashValue & (1 << i)) == 0 {
			leadingZeros++
		} else {
			break
		}
	}
	hyperloglog.Registers[index] = int(math.Max(float64(hyperloglog.Registers[index]), float64(leadingZeros+1)))
}

func leadingZeroCount(n uint64) int {
	return bits.LeadingZeros64(n) + 1
}
