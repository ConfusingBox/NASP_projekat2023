package strukture

import (
	hashfunc "NASP_projekat2023/utils"
	"encoding/binary"
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

func (countminsketch *CountMinSketch) SerializeCMS(filepath string) ([]byte, error) {
	widthBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(widthBytes, uint64(countminsketch.Width))

	depthBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(depthBytes, uint64(countminsketch.Depth))

	greskaBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(greskaBytes, math.Float64bits(float64(countminsketch.Greska)))

	gammaBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(gammaBytes, math.Float64bits(float64(countminsketch.Gamma)))

	matrixBytes := make([]byte, countminsketch.Width*countminsketch.Depth*8)
	for i := 0; i < countminsketch.Depth; i++ {
		for j := 0; j < countminsketch.Width; j++ {
			offset := (i*countminsketch.Width + j) * 8
			binary.BigEndian.PutUint64(matrixBytes[offset:offset+8], uint64(countminsketch.Matrica[i][j]))
		}
	}

	returnArray := append(widthBytes, depthBytes...)
	returnArray = append(returnArray, greskaBytes...)
	returnArray = append(returnArray, gammaBytes...)
	returnArray = append(returnArray, matrixBytes...)

	return returnArray, nil
}

func DeserializeCMS(data []byte) (*CountMinSketch, error) {
	width := int(binary.BigEndian.Uint64(data[:8]))

	depth := int(binary.BigEndian.Uint64(data[8:16]))

	greska := math.Float64frombits(binary.BigEndian.Uint64(data[16:24]))

	gamma := math.Float64frombits(binary.BigEndian.Uint64(data[24:32]))

	matrix := make([][]int, depth)
	for i := 0; i < depth; i++ {
		matrix[i] = make([]int, width)
		for j := 0; j < width; j++ {
			offset := 32 + (i*width+j)*8
			matrix[i][j] = int(binary.BigEndian.Uint64(data[offset : offset+8]))
		}
	}

	return &CountMinSketch{
		Width:   width,
		Depth:   depth,
		Greska:  float32(greska),
		Gamma:   float32(gamma),
		Matrica: matrix,
	}, nil
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
