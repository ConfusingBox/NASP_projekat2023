package proj

import "hash/fnv"

func bloomflh1(s string, arrSize int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32()) % arrSize
}

func bloomflh2(s string, arrSize int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return (int(h.Sum32()) * 2) % arrSize
}

func bloomflh3(s string, arrSize int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return (int(h.Sum32()) * 3) % arrSize
}

func bloomflh4(s string, arrSize int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return (int(h.Sum32()) * 4) % arrSize
}
