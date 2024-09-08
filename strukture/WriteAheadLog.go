package strukture

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	WAL_PUT    = 0
	WAL_DELETE = 1
)

type WriteAheadLog struct {
	index       int64
	currentFile *os.File
	segmentSize int64 // How many entries are allowed in a single segment
	currentSize int64
}

func CreateWriteAheadLog(segmentSize int64) (*WriteAheadLog, error) {
	err := os.MkdirAll("data/wal", os.ModePerm)
	if err != nil {
		return nil, err
	}

	index, err := FindCurrentWriteAheadLogIndex()
	if err != nil {
		return nil, err
	}

	currentFile, err := os.OpenFile("data/wal/wal_"+strconv.Itoa(int(index))+".bin", os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	fileInfo, err := currentFile.Stat()
	if err != nil {
		return nil, err
	}
	currentSize := fileInfo.Size()

	return &WriteAheadLog{index, currentFile, segmentSize, currentSize}, nil
}

func FindCurrentWriteAheadLogIndex() (int64, error) {
	// Returns the largest WAL data file index of all existing WAL data files.

	files, err := os.ReadDir("data/wal")
	if err != nil {
		return 0, err
	}

	var maxIndex int64 = 0

	for _, file := range files {
		fileName := file.Name()
		indexInName := strings.Split(fileName, "wal_")[1]
		indexInName = strings.Split(indexInName, ".bin")[0]

		index, err := strconv.ParseInt(string(indexInName), 10, 64)
		if err != nil {
			return 0, err
		}

		if index > maxIndex {
			maxIndex = index
		}
	}

	if maxIndex == 0 {
		if _, err := os.Stat("data/wal/wal_0.bin"); errors.Is(err, os.ErrNotExist) {
			os.Create("data/wal/wal_0.bin")
		}
	}

	return maxIndex, nil
}

func (writeAheadLog *WriteAheadLog) Log(entry *Entry) error {
	if entry.Size() > writeAheadLog.segmentSize-writeAheadLog.currentSize {
		err := writeAheadLog.Dump()
		if err != nil {
			return err
		}
	}

	var key string
	array := entry.ToByteArray()
	if int64(len(array)) != 29+int64(entry.keySize)+int64(entry.valueSize) {
		fmt.Print(array)
		fmt.Scan(&key)
	}

	_, err := writeAheadLog.currentFile.Write(entry.ToByteArray())
	if err != nil {
		return err
	}

	writeAheadLog.currentSize += entry.Size()

	return nil
}

func (writeAheadLog *WriteAheadLog) Dump() error {
	err := writeAheadLog.currentFile.Close()
	if err != nil {
		return err
	}

	writeAheadLog.index++

	_, err = os.Create("data/wal/wal_" + strconv.Itoa(int(writeAheadLog.index)) + ".bin")
	if err != nil {
		return err
	}

	writeAheadLog.currentFile, err = os.OpenFile("data/wal/wal_"+strconv.FormatInt(writeAheadLog.index, 10)+".bin", os.O_RDWR, 0777)
	if err != nil {
		return err
	}

	writeAheadLog.currentSize = 0

	return nil
}

func WriteAheadLogTest() {
	wal, _ := CreateWriteAheadLog(128)

	e1 := CreateEntry("1", []byte{0, 1, 2, 3, 4}, 0)
	e2 := CreateEntry("02", []byte("abcde"), 1)
	e3 := CreateEntry("003", []byte("Test string"), 0)

	wal.Log(e1)
	wal.Log(e2)
	wal.Log(e3)
}
