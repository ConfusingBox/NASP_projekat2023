package strukture

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"regexp"
	"strings"

	config "NASP_projekat2023/utils"
	hashfunc "NASP_projekat2023/utils"
)

type SimHash struct {
	data        string
	hashLength  int64
	fingerprint []int
}

func NewSimHash(data string) (*SimHash, error) {
	config, err := config.LoadConfigValues("config.json")
	if err != nil {
		return nil, err
	}

	return &SimHash{data, config.SimHashHashSize, nil}, nil
}

func NewSimHashWithFingerprint(data string, hashLength int) (*SimHash, error) {
	// Vraca SimHash u kojem je fingerprint vec izracunat

	sh, err := NewSimHash(data)

	sh.calculateFingerprint()

	return sh, err
}

func (sh *SimHash) calculateFingerprint() {
	hashLength := sh.hashLength
	fingerprint := make([]int, hashLength)
	words := strings.Split(sh.data, " ")

	// fmt.Println(words)
	for _, word := range words {
		word = regexp.MustCompile("[^a-zA-Z0-9 ]+").ReplaceAllString(word, "")
		// fmt.Print(word, hashLength)
		hash := hashfunc.StringBinaryHash(word, hashLength)
		for i := int64(0); i < hashLength; i++ {
			if int(hash[i]) == 48 {
				fingerprint[i]--
			} else {
				fingerprint[i]++
			}
		}
		// fmt.Println(word, hash)
	}
	for i := int64(0); i < hashLength; i++ {
		if fingerprint[i] > 0 {
			fingerprint[i] = 1
		} else {
			fingerprint[i] = 0
		}
	}
	fmt.Println("Finished calculating fingerprint:", fingerprint)
	sh.fingerprint = fingerprint
}

func (sh1 *SimHash) hammingDistance(sh2 *SimHash) int {
	hammingDistance := 0

	for i, value := range sh1.fingerprint {
		if value != sh2.fingerprint[i] {
			hammingDistance++
		}
	}

	return hammingDistance
}

func serializeSimHash(sh *SimHash) ([]byte, error) {
	data := []byte(sh.data)
	hashLength := int32(sh.hashLength)
	fingerprint := make([]int32, len(sh.fingerprint))
	for i, v := range sh.fingerprint {
		fingerprint[i] = int32(v)
	}

	var buffer bytes.Buffer

	if err := binary.Write(&buffer, binary.BigEndian, int32(len(data))); err != nil {
		return nil, err
	}

	if err := binary.Write(&buffer, binary.BigEndian, data); err != nil {
		return nil, err
	}

	if err := binary.Write(&buffer, binary.BigEndian, hashLength); err != nil {
		return nil, err
	}

	if err := binary.Write(&buffer, binary.BigEndian, int32(len(fingerprint))); err != nil {
		return nil, err
	}

	for _, v := range fingerprint {
		if err := binary.Write(&buffer, binary.BigEndian, v); err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

func deserializeSimHash(data []byte) (*SimHash, error) {
	var sh SimHash

	var dataLength int32
	var hashLength int32
	var fingerprintLength int32

	reader := bytes.NewReader(data)

	if err := binary.Read(reader, binary.BigEndian, &dataLength); err != nil {
		return nil, err
	}

	dataBytes := make([]byte, dataLength)
	if err := binary.Read(reader, binary.BigEndian, &dataBytes); err != nil {
		return nil, err
	}
	sh.data = string(dataBytes)

	if err := binary.Read(reader, binary.BigEndian, &hashLength); err != nil {
		return nil, err
	}
	sh.hashLength = int64(hashLength)

	if err := binary.Read(reader, binary.BigEndian, &fingerprintLength); err != nil {
		return nil, err
	}

	sh.fingerprint = make([]int, fingerprintLength)
	for i := 0; i < int(fingerprintLength); i++ {
		var value int32
		if err := binary.Read(reader, binary.BigEndian, &value); err != nil {
			return nil, err
		}
		sh.fingerprint[i] = int(value)
	}

	return &sh, nil
}

func (sh *SimHash) toString() string {
	return sh.data
}

// func main() {
// 	sh1 := NewSimHashWithFingerprint("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc mollis lorem et tortor elementum, ac laoreet nisi finibus. Pellentesque consectetur felis turpis. Nulla consequat nibh ut mauris semper vehicula. Donec quis massa a dui rutrum dapibus. Sed a commodo eros. Phasellus venenatis ligula eget lorem tempus, eu molestie erat sagittis. Aenean auctor urna quis orci rhoncus, pellentesque ultrices ex ullamcorper. Curabitur arcu sem, laoreet vel sem ut, blandit vulputate mauris. Aenean posuere leo quam, vitae venenatis dolor bibendum placerat.", 8)
// 	sh2 := NewSimHash("Nullam nunc odio, rutrum laoreet nisl non, venenatis lobortis risus. In lobortis lacus non malesuada varius. Aliquam pellentesque ligula at nibh gravida interdum. Suspendisse et ultricies dolor, ac rhoncus lectus. Donec varius ex eu turpis luctus pharetra. Aliquam blandit, nulla vel malesuada condimentum, tortor nisi sodales erat, ac semper leo lacus ut dui. Pellentesque consequat commodo massa et tempor. Suspendisse mattis, quam nec vulputate imperdiet, mauris felis iaculis risus, in tincidunt lorem elit eu purus. Nullam elementum neque mattis felis convallis, ut consequat leo aliquet.", 8)
// 	sh1.calculateFingerprint()
// 	sh2.calculateFingerprint()

// 	fmt.Println("\nHamming distance sh1-sh2: ", sh1.hammingDistance(sh2))

// 	serializedSh1, err := serializeSimHash(sh1)
// 	if err != nil {
// 		fmt.Println("Error during serialization:", err)
// 		return
// 	}
// 	fmt.Println(serializedSh1, "\n", len(serializedSh1))

// 	sh3, err := deserializeSimHash(serializedSh1)
// 	if err != nil {
// 		fmt.Println("Error during deserialization:", err)
// 		return
// 	}

// 	fmt.Println("\nDeserialized sh3 fingerprint:", sh3.fingerprint)
// 	fmt.Println("\nDeserialized sh3 data:", sh3.toString())

// 	fmt.Print("\nHamming distance sh1-sh3: ", sh3.hammingDistance(sh1))
// 	fmt.Print("\nHamming distance sh2-sh3: ", sh3.hammingDistance(sh2))

// }
