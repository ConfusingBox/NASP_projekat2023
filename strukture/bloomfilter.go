package strukture

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	hashfunc "NASP_projekat2023/utils"
)

type BloomFilter struct {
	BitArray []byte
	NumHash  int64
}

func NewBloomFilterWithSize(expectedElements int64, falsePositiveRate float64) *BloomFilter {
	size := calculateSize(expectedElements, falsePositiveRate)
	numHash := calculateNumHash(expectedElements, int64(size))
	return &BloomFilter{
		BitArray: make([]byte, size),
		NumHash:  int64(numHash),
	}
}

func (bloomFilter *BloomFilter) Serialize() []byte {
	sizeByteHash := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeByteHash, uint64(bloomFilter.NumHash))

	returnArray := append(sizeByteHash, bloomFilter.BitArray...)

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
		NumHash:  int64(numHash),
		BitArray: bitArray,
	}, nil
}

func (bf *BloomFilter) Delete() {
	bf.BitArray = make([]byte, len(bf.BitArray))
}

func (bf *BloomFilter) Lookup(s string) bool {
	for i := 0; i < int(bf.NumHash); i++ {
		index := hashfunc.CustomHash(s, len(bf.BitArray), i)
		if bf.BitArray[index]>>7&1 != 1 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Insert(s string) {
	for i := 0; i < int(bf.NumHash); i++ {
		index := hashfunc.CustomHash(s, len(bf.BitArray), i)
		bf.BitArray[index] |= 1 << 7
	}
}

func calculateSize(expectedElements int64, falsePositiveRate float64) int {
	return int(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func calculateNumHash(expectedElements, size int64) int {
	return int(math.Ceil((float64(size) / float64(expectedElements)) * math.Log(2)))
}

func BloomFilterMenu(bf *BloomFilter) {
	for {
		fmt.Println("1. Pronadji element")
		fmt.Println("2. Dodaj element")
		fmt.Println("3. Obriši sve iz bloomfilter-a")
		fmt.Println("x. Close")

		var choice string
		fmt.Print("Unesite opciju: ")
		fmt.Scan(&choice)

		switch strings.ToLower(choice) {
		case "1":
			var key string
			fmt.Print("Unesite element za pronalazak: ")
			fmt.Scan(&key)
			if bf.Lookup(key) {
				fmt.Println("element je možda pronađen.")
			} else {
				fmt.Println("element zasigurno nije pronađen.")
			}
		case "2":
			var key string
			fmt.Print("Unesite šta želite da ubacite: ")
			fmt.Scan(&key)
			bf.Insert(key)
			fmt.Println("element je dodat u bloomfilter")
			//SerializeBloomFilter(bf)
		case "3":
			var choice2 string
			fmt.Print("Da li ste sigurni?\n1. Da\n2. Ne")
			fmt.Scan(&choice2)
			switch strings.ToLower(choice2) {
			case "1":
				bf.Delete()
				fmt.Println("Svi elementi u bloomfilter-u su postavljeni na 0")
			case "2":
				fmt.Println("Bloomfilter se nije resetovao")
			default:
				fmt.Println("Pogrešan unos")
			}
		case "x":
			fmt.Println("Izlazak iz BloomFilter menija.")
			return
		default:
			fmt.Println("Pogrešan unos")
		}
	}
}
