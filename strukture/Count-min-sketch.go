package strukture

import (
	hashfunc "NASP_projekat2023/utils"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

type CountMinSketch struct {
	Width   int64
	Depth   int64
	Greska  float64 // 0 < greska < 1, Sto je manje bolje je
	Gamma   float64 // 0 < gamma < 1, Sto je vece bolje je
	Matrica [][]int
}

func NewCountMinSketch(width, depth int64) *CountMinSketch {
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
	for i := int64(0); i < countminsketch.Depth; i++ {
		index := hashfunc.CustomHash(item, int(countminsketch.Width), int(i))
		countminsketch.Matrica[i][index]++
	}
}

func (countminsketch *CountMinSketch) SerializeCMS() []byte {
	widthBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(widthBytes, uint64(countminsketch.Width))

	depthBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(depthBytes, uint64(countminsketch.Depth))

	greskaBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(greskaBytes, math.Float64bits(float64(countminsketch.Greska)))

	gammaBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(gammaBytes, math.Float64bits(float64(countminsketch.Gamma)))

	matrixBytes := make([]byte, countminsketch.Width*countminsketch.Depth*8)
	for i := int64(0); i < countminsketch.Depth; i++ {
		for j := int64(0); j < countminsketch.Width; j++ {
			offset := (i*countminsketch.Width + j) * 8
			binary.BigEndian.PutUint64(matrixBytes[offset:offset+8], uint64(countminsketch.Matrica[i][j]))
		}
	}

	returnArray := append(widthBytes, depthBytes...)
	returnArray = append(returnArray, greskaBytes...)
	returnArray = append(returnArray, gammaBytes...)
	returnArray = append(returnArray, matrixBytes...)

	return returnArray
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
		Width:   int64(width),
		Depth:   int64(depth),
		Greska:  float64(greska),
		Gamma:   float64(gamma),
		Matrica: matrix,
	}, nil
}

func (countminsketch *CountMinSketch) Count(item string) int {
	var pojave int = math.MaxInt

	for i := int64(0); i < countminsketch.Depth; i++ {
		index := hashfunc.CustomHash(item, int(countminsketch.Width), int(i))
		if countminsketch.Matrica[i][index] < pojave {
			pojave = countminsketch.Matrica[i][index]
		}
	}

	return pojave
}

func CMSMenu(cms *CountMinSketch) {
	for {
		fmt.Println("1. Dodaj element")
		fmt.Println("2. Broj pojavljivanja elementa")
		fmt.Println("3. Obrisi sve")
		fmt.Println("x. Zatvori")

		var choice string
		fmt.Print("Unesite opciju: ")
		fmt.Scan(&choice)

		switch strings.ToLower(choice) {
		case "1":
			var item string
			fmt.Print("Unesite element za dodavanje: ")
			fmt.Scan(&item)
			cms.Add(item)
			fmt.Println("Element dodat u Count-Min Sketch.")
		case "2":
			var item string
			fmt.Print("Unesite element za pretragu: ")
			fmt.Scan(&item)
			count := cms.Count(item)
			fmt.Printf("Element '%s' se pojavljuje %d puta.\n", item, count)
			//cms.SerializeCMS()
		case "3":
			var choice2 string
			fmt.Print("Da li ste sigurni?\n1. Da\n2. Ne\n")
			fmt.Scan(&choice2)
			switch strings.ToLower(choice2) {
			case "1":
				deleteCountMiNSketch(cms)
				fmt.Println("Sve vrednosti u Count-Min Sketch su postavljene na 0.")
			case "2":
				fmt.Println("Count-Min Sketch se nije resetovao.")
			default:
				fmt.Println("Pogresan unos.")
			}
		case "x":
			fmt.Println("Izlazak iz Count-Min Sketch menija.")
			return
		default:
			fmt.Println("Pogresan unos. Molimo pokusajte ponovo.")
		}
	}
}
