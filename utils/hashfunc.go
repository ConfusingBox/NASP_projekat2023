package utils

import "hash/fnv"

func Hash1(s string, arrSize int) int {
	hash := fnv.New32a()
	hash.Write([]byte(s))
	return int(hash.Sum32()) % arrSize
}

func Hash2(s string, arrSize int) int {
	hash := fnv.New32a()
	hash.Write([]byte(s))
	return (int(hash.Sum32()) * 2) % arrSize
}

func Hash3(s string, arrSize int) int {
	hash := fnv.New32a()
	hash.Write([]byte(s))
	return (int(hash.Sum32()) * 3) % arrSize
}

func Hash4(s string, arrSize int) int {
	hash := fnv.New32a()
	hash.Write([]byte(s))
	return (int(hash.Sum32()) * 4) % arrSize
}
