package strukture

import (
	hashfunc "NASP_projekat2023/utils"
	"encoding/gob"
	"math"
	"os"
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

func (countminsketch *CountMinSketch) SerializeCMS(filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(countminsketch); err != nil {
		return err
	}

	return nil
}

func DeserializeCMS(filepath string) (*CountMinSketch, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var countminsketch CountMinSketch
	decoder := gob.NewDecoder(file)

	if err := decoder.Decode(&countminsketch); err != nil {
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
