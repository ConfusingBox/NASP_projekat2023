package proj

import (
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

func (sketch *CountMinSketch) Add(item string) {
	h1 := bloomflh1(item, sketch.Width)
	h2 := bloomflh2(item, sketch.Width)
	h3 := bloomflh3(item, sketch.Width)
	h4 := bloomflh4(item, sketch.Width)
	for i := 0; i < sketch.Depth; i++ {
		index := (h1 + i*h2 + i*i*h3 + i*i*i*h4)
		sketch.Matrica[i][index]++
	}
}

func (CMS *CountMinSketch) SerializeCMS() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(CMS)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func DeserializeCMS(data []byte) (*CountMinSketch, error) {
	var CMS CountMinSketch
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	err := decoder.Decode(&CMS)
	if err != nil {
		return nil, err
	}
	return &CMS, nil
}

func (sketch *CountMinSketch) Count(item string) int {
	h1 := bloomflh1(item, sketch.Width)
	h2 := bloomflh2(item, sketch.Width)
	h3 := bloomflh3(item, sketch.Width)
	h4 := bloomflh4(item, sketch.Width)
	var pojave int = 0
	for i := 0; i < sketch.Depth; i++ {
		index := (h1 + i*h2 + i*i*h3 + i*i*i*h4)
		pojave += sketch.Matrica[i][index]
	}
	return pojave
}
