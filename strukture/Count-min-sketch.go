package strukture

import (
	hashfunc "NASP_projekat2023/utils"
	"bytes"
	"encoding/gob"
)

type CountMinSketch struct {
	Width   int
	Depth   int
	Greska  float32 //0<greska<1 sto manje to bolje
	Gamma   float32 //0<gamma<1 sto vece to bolje
	Matrica [][]int
}

func NewCountMinSketch(width, depth int) *CountMinSketch {
	matrica := make([][]int, depth)
	for i := range matrica {
		matrica[i] = make([]int, width)
	}
	return &CountMinSketch{Width: width, Depth: depth, Matrica: matrica}
}

func deleteCountMiNSketch(sketch *CountMinSketch) {
	for i := range sketch.Matrica {
		for j := range sketch.Matrica[i] {
			sketch.Matrica[i][j] = 0
		}
	}
}

func (countminsketch *CountMinSketch) Add(item string) {
	hash1 := hashfunc.Hash1(item, countminsketch.Width)
	hash2 := hashfunc.Hash2(item, countminsketch.Width)
	hash3 := hashfunc.Hash3(item, countminsketch.Width)
	hash4 := hashfunc.Hash4(item, countminsketch.Width)
	for i := 0; i < countminsketch.Depth; i++ {
		index := (hash1 + i*hash2 + i*i*hash3 + i*i*i*hash4)
		countminsketch.Matrica[i][index]++
	}
}

func (countminsketch *CountMinSketch) SerializeCMS() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(countminsketch)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func DeserializeCMS(data []byte) (*CountMinSketch, error) {
	var countminsketch CountMinSketch
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	err := decoder.Decode(&countminsketch)
	if err != nil {
		return nil, err
	}
	return &countminsketch, nil
}

func (countminsketch *CountMinSketch) Count(item string) int {
	hash1 := hashfunc.Hash1(item, countminsketch.Width)
	hash2 := hashfunc.Hash2(item, countminsketch.Width)
	hash3 := hashfunc.Hash3(item, countminsketch.Width)
	hash4 := hashfunc.Hash4(item, countminsketch.Width)
	var pojave int = 0
	for i := 0; i < countminsketch.Depth; i++ {
		index := (hash1 + i*hash2 + i*i*hash3 + i*i*i*hash4)
		pojave += countminsketch.Matrica[i][index]
	}
	return pojave
}
