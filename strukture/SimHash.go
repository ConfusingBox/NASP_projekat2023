package strukture

import (
	"bytes"
	"encoding/gob"
	"regexp"
	"strings"
)

type SimHash struct {
	data        string
	hashLength  int
	fingerprint []int
}

func NewSimHash(data string, hashLength int) *SimHash {
	// Potrebno je ucitati hashLength iz config.json fajla.

	return &SimHash{data, hashLength, nil}
}

func NewSimHashWithFingerprint(data string, hashLength int) *SimHash {
	// Vraca SimHash u kojem je fingerprint vec izracunat

	sh := NewSimHash(data, hashLength)
	sh.calculateFingerprint()

	return sh
}

func (sh *SimHash) calculateFingerprint() {
	hashLength := sh.hashLength
	fingerprint := make([]int, hashLength)
	words := strings.Split(sh.data, " ")

	for _, word := range words {
		word = regexp.MustCompile("[^a-zA-Z0-9 ]+").ReplaceAllString(word, "")
		hash := stringBinaryHash(word, hashLength)

		for i := 0; i < hashLength; i++ {
			if int(hash[i]) == 48 {
				fingerprint[i]--
			} else {
				fingerprint[i]++
			}
		}
	}

	for i := 0; i < hashLength; i++ {
		if fingerprint[i] > 0 {
			fingerprint[i] = 1
		} else {
			fingerprint[i] = 0
		}
	}

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

func (sh *SimHash) serializeSimHash() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(sh)

	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func deserializeSimHash(data []byte) (*SimHash, error) {
	var sh SimHash
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	err := decoder.Decode(&sh)

	if err != nil {
		return nil, err
	}

	return &sh, nil
}

/*
func main() {
	sh1 := NewSimHashWithFingerprint("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc mollis lorem et tortor elementum, ac laoreet nisi finibus. Pellentesque consectetur felis turpis. Nulla consequat nibh ut mauris semper vehicula. Donec quis massa a dui rutrum dapibus. Sed a commodo eros. Phasellus venenatis ligula eget lorem tempus, eu molestie erat sagittis. Aenean auctor urna quis orci rhoncus, pellentesque ultrices ex ullamcorper. Curabitur arcu sem, laoreet vel sem ut, blandit vulputate mauris. Aenean posuere leo quam, vitae venenatis dolor bibendum placerat.", 8)
	sh2 := NewSimHash("Nullam nunc odio, rutrum laoreet nisl non, venenatis lobortis risus. In lobortis lacus non malesuada varius. Aliquam pellentesque ligula at nibh gravida interdum. Suspendisse et ultricies dolor, ac rhoncus lectus. Donec varius ex eu turpis luctus pharetra. Aliquam blandit, nulla vel malesuada condimentum, tortor nisi sodales erat, ac semper leo lacus ut dui. Pellentesque consequat commodo massa et tempor. Suspendisse mattis, quam nec vulputate imperdiet, mauris felis iaculis risus, in tincidunt lorem elit eu purus. Nullam elementum neque mattis felis convallis, ut consequat leo aliquet.", 8)

	sh1.calculateFingerprint()
	sh2.calculateFingerprint()

	fmt.Print("\nHamming distance sh1-sh2: ", sh1.hammingDistance(sh2))

	var sh3 *SimHash
	var data []byte

	data, _ = sh1.serializeSimHash()
	sh3, _ = deserializeSimHash(data)

	sh3.calculateFingerprint() // Ovdje nastaje greska. sh3 se vraca kao nil.

	fmt.Print("\nHamming distance sh1-sh3: ", sh3.hammingDistance(sh1))
	fmt.Print("\nHamming distance sh2-sh3: ", sh3.hammingDistance(sh2))
}
*/
