package main

import (
	"NASP_projekat2023/strukture"
	"fmt"
	"os"
	"strings"
)

func main() {
	// Initialize the data structures
	expectedElements := 1000
	falsePositiveRate := 0.01
	bf := strukture.NewBloomFilterWithSize(expectedElements, falsePositiveRate)

	width := 1000
	depth := 5
	cms := strukture.NewCountMinSketch(width, depth)

	precision := 12
	hll := strukture.NewHyperLogLog(precision)

	// SimHash initialization commented out
	/*
		data := "NestoNesto"
		sh, err := strukture.NewSimHashWithFingerprint(data, 64)
		if err != nil {
			fmt.Println("Err:", err)
			return
		}
	*/

	for {
		fmt.Println("Main Menu:")
		fmt.Println("1. Bloom Filter")
		fmt.Println("2. Count-Min Sketch")
		fmt.Println("3. HyperLogLog")
		fmt.Println("4. SimHash")
		fmt.Println("x. Exit")

		var choice string
		fmt.Print("Enter your choice: ")
		fmt.Scan(&choice)

		switch strings.ToLower(choice) {
		case "1":
			strukture.BloomFilterMenu(bf)
		case "2":
			strukture.CMSMenu(cms)
		case "3":
			strukture.HLLMenu(hll)
		case "4":
			// Uncomment if SimHash functionality is required
			/*
				data := "Some text for SimHash"
				sh, err := strukture.NewSimHashWithFingerprint(data, 64)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				strukture.SimHashMenu(sh)
			*/
		case "x":
			os.Exit(0)
		default:
			fmt.Println("Invalid choice. Please try again.")
		}
	}
}
