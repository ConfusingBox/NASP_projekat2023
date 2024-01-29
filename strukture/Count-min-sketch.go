package strukture

import (
	hashfunc "NASP_projekat2023/utils"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"math"
)

type CountMinSketch struct {
	Width   int
	Depth   int
	Greska  float32 // 0 < greska < 1, Sto je manje bolje je
	Gamma   float32 // 0 < gamma < 1, Sto je vece bolje je
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
	for i := 0; i < countminsketch.Depth; i++ {
		index := hashfunc.CustomHash(item, countminsketch.Width, i)
		countminsketch.Matrica[i][index]++
	}
}

func (countminsketch *CountMinSketch) SerializeCMS() ([]byte, error) {
	var buffer bytes.Buffer

	if err := binary.Write(&buffer, binary.BigEndian, countminsketch.Width); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.BigEndian, countminsketch.Depth); err != nil {
		return nil, err
	}

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

	if err := binary.Read(buffer, binary.BigEndian, &countminsketch.Width); err != nil {
		return nil, err
	}
	if err := binary.Read(buffer, binary.BigEndian, &countminsketch.Depth); err != nil {
		return nil, err
	}

	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&countminsketch)
	if err != nil {
		return nil, err
	}
	return &countminsketch, nil
}

func (countminsketch *CountMinSketch) Count(item string) int {
	var pojave int = math.MaxInt

	for i := 0; i < countminsketch.Depth; i++ {
		index := hashfunc.CustomHash(item, countminsketch.Width, i)
		if countminsketch.Matrica[i][index] < pojave {
			pojave = countminsketch.Matrica[i][index]
		}
	}

	return pojave
}
