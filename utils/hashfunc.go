package utils

import (
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math"
)

func CustomHash(s string, arrSize, index int) int {
	hash1 := hash1(s, arrSize)
	hash2 := hash2(s, arrSize)
	hash3 := hash3(s, arrSize)
	hash4 := hash4(s, arrSize)

	combinedHash := (hash1 + index*hash2 + index*index*hash3 + index*index*index*hash4) % arrSize
	if combinedHash < 0 {
		combinedHash += arrSize //Ovo osigurava da ne bude negativan idx
	}
	return combinedHash

}

func hash1(s string, arrSize int) int {
	hasher := fnv.New32()
	hasher.Write([]byte(s))
	return int(hasher.Sum32()) % arrSize
}

func hash2(s string, arrSize int) int {
	hasher := fnv.New32a()
	hasher.Write([]byte(s))
	return int(hasher.Sum32()) % arrSize
}

func hash3(s string, arrSize int) int {
	hasher := crc32.NewIEEE()
	hasher.Write([]byte(s))
	return int(hasher.Sum32()) % arrSize
}

func hash4(s string, arrSize int) int {
	hasher := fnv.New64()
	hasher.Write([]byte(s))
	return int(hasher.Sum64()) % arrSize
}

func stringBinaryHash(str string, hashLength int) string {
	sum := 0

	for i, char := range str {
		sum += int(char) * int(math.Pow(53, float64(i)))
	}

	sum %= int(math.Pow(2, float64(hashLength))) - 1
	hash := fmt.Sprintf("%b", sum)

	for len(hash) != hashLength {
		hash = "0" + hash
	}

	return hash
}
