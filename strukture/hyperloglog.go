package strukture

import (
	hashfunc "NASP_projekat2023/utils"
	"encoding/binary"
	"math"
	"math/bits"
)

type HyperLogLog struct {
	Precision int
	Registers []int
}

func NewHyperLogLog(precision int) *HyperLogLog {
	size := 1 << precision
	return &HyperLogLog{precision, make([]int, size)}
}

func Delete(hll *HyperLogLog) {
	for i := range hll.Registers {
		hll.Registers[i] = 0
	}
}

func (hyperloglog *HyperLogLog) SerializeHLL() []byte {
	sizeBytePrecision := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBytePrecision, uint64(hyperloglog.Precision))
	byteSlice := make([]byte, len(hyperloglog.Registers))
	for i, v := range hyperloglog.Registers {
		byteSlice[i] = byte(v)
	}

	returnArray := append(sizeBytePrecision, byteSlice...)

	return returnArray
}

func DeserializeHLL(data []byte) (*HyperLogLog, error) {
	precisionBytes := data[:8]
	precision := int(binary.BigEndian.Uint64(precisionBytes))

	remainingBytes := data[8:]
	registerSlice := make([]int, len(remainingBytes))
	for i, v := range remainingBytes {
		registerSlice[i] = int(v)
	}
	return &HyperLogLog{
		Precision: precision,
		Registers: registerSlice,
	}, nil

}

func (hyperloglog *HyperLogLog) Estimate() float64 {
	alpha := getAlpha(hyperloglog.Precision)
	sumInverse := 0.0

	for _, value := range hyperloglog.Registers {
		sumInverse += 1.0 / math.Pow(2.0, float64(value))
	}

	estimate := alpha * float64(len(hyperloglog.Registers)*len(hyperloglog.Registers)) / sumInverse

	if estimate <= 2.5*float64(len(hyperloglog.Registers)) {
		zeroCount := 0

		for _, value := range hyperloglog.Registers {
			if value == 0 {
				zeroCount++
			}
		}

		if zeroCount != 0 {
			correction := linearCountingCorrection(float64(len(hyperloglog.Registers)), float64(zeroCount))
			estimate = correction
		}
	}

	return estimate
}

func getAlpha(precision int) float64 {
	const defaultAlpha = 0.7213
	switch precision {
	case 4:
		return defaultAlpha * 0.98
	case 5:
		return defaultAlpha * 0.99
	case 6:
		return defaultAlpha * 1.01
	default:
		return defaultAlpha / math.Pow(2.0, float64(precision))
	}
}

func linearCountingCorrection(m, v float64) float64 {
	return m * math.Log(m/v)
}

func (hyperloglog *HyperLogLog) Add(item string) {
	for i := 0; i < 4; i++ {
		index := int(hashfunc.CustomHash(item, 1<<hyperloglog.Precision, i))
		leadingZeros := leadingZeroCount(uint64(hashfunc.CustomHash(item, 1<<hyperloglog.Precision, i)))

		currentValue := float64(hyperloglog.Registers[index])
		newValue := float64(leadingZeros)
		alpha := 0.1
		hyperloglog.Registers[index] = int(alpha*currentValue + (1-alpha)*newValue)
	}
}
func leadingZeroCount(n uint64) int {
	return bits.LeadingZeros64(n) + 1
}
