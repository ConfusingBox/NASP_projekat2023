package strukture

import (
	"bytes"
	"fmt"
	"math/rand"
)

// SkipListNode represents a node in the SkipList.
type SkipListNode struct {
	key   string        // The key stored in the node.
	entry Entry         // The value associated with the key.
	right *SkipListNode // Pointer to the next node in the same level.
	down  *SkipListNode // Pointer to the node below in the next level.
	level int64         // The level of the node.
}

// SkipList represents a SkipList data structure.
type SkipList struct {
	head     *SkipListNode // The head node of the SkipList.
	maxLevel int64         // The maximum level of the SkipList.
}

// NewSkipList creates a new SkipList.
func CreateSkipList(maxLevel int64) *SkipList {
	return &SkipList{
		maxLevel: maxLevel,
	}
}

// roll generates a random level for a new node.
func (s *SkipList) roll() int64 {
	var level int64 = 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxLevel {
			return level
		}
	}
	return level
}

// Insert inserts a key-value pair into the SkipList.
func (s *SkipList) Insert(entry Entry) bool {
	level := s.roll()
	newNode := &SkipListNode{key: entry.key, entry: entry, level: level}
	if s.head == nil {
		s.head = newNode
	} else {
		node := s.head
		for node.right != nil && bytes.Compare([]byte(node.right.key), []byte(entry.key)) < 0 {
			node = node.right
		}
		newNode.right = node.right
		node.right = newNode
	}
	return true
}

// Search searches for a key in the SkipList.
// It returns the node containing the key if it exists, otherwise it returns nil.
func (s *SkipList) Get(key string) *Entry {
	for node := s.head; node != nil; node = node.down {
		for node.right != nil && bytes.Compare([]byte(node.right.key), []byte(key)) <= 0 {
			node = node.right
		}
		if bytes.Equal([]byte(node.key), []byte(key)) {
			return &node.entry
		}
	}
	return nil
}

// Delete deletes a key from the SkipList.
func (s *SkipList) Delete(key string) bool {
	found := false
	for h := s.head; h != nil; h = h.down {
		if bytes.Equal([]byte(h.key), []byte(key)) {
			s.head = h.right
			found = true
		} else {
			for node := h; node.right != nil; node = node.right {
				if bytes.Equal([]byte(node.right.key), []byte(key)) {
					node.right = node.right.right
					found = true
					break
				}
			}
		}
	}
	// Naredne 4 linije koda su ekvivalentne sa "return found" btw XD
	if !found {
		return false
	}
	return true
}

// Print prints the keys and values of the SkipList.
func (s *SkipList) Print() {
	for node := s.head; node != nil; node = node.down {
		for n := node; n != nil; n = n.right {
			fmt.Printf("Key: %s, Value: %s, Timestamp: %s, Tombstone: %v\n", string(n.key), string(n.entry.value), n.entry.timestamp, n.entry.tombstone)
		}
		fmt.Println()
	}
}

/*
	func SkipListMenu(s *SkipList) {
		scanner := bufio.NewScanner(os.Stdin)

		for {
			fmt.Println("1. Ubacite element")
			fmt.Println("2. Pretražite element")
			fmt.Println("3. Izbrišite element")
			fmt.Println("4. Prikaži skiplist-u")
			fmt.Println("x. Izlaz")
			fmt.Print("Unesite opciju: ")

			choice := strings.TrimSpace(scanner.Text())

			switch choice {
			case "1":
				fmt.Print("Unesite ključ: ")
				key := scanner.Text()

				fmt.Print("Unesite vrednost: ")
				value := scanner.Text()

				entry := NewMemtableEntry([]byte(key), []byte(value), false)
				s.Insert(*entry)
			case "2":
				fmt.Print("Upišite ključ za pretragu: ")
				key := scanner.Text()

				node := s.Search([]byte(key))
				if node != nil {
					fmt.Printf("Ključ %s sa vrednošću %s\n", node.Key, node.Value)
				} else {
					fmt.Printf("Ključ %s nije pronađen\n", key)
				}
			case "3":
				fmt.Print("Unesite ključ za brisanje: ")
				key := scanner.Text()

				s.Delete([]byte(key))
				fmt.Printf("Ključ %s je izbrisan.\n", key)
			case "4":
				s.Print()
			case "5":
				return
			default:
				fmt.Println("Pogrešna opcija, pokušajte ponovo")
			}
		}
	}

func SkipListMain() {
	// Create a new SkipList
	s := CreateSkipList(3)

	// Keys and values for testing
	keys := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		[]byte("d"),
		[]byte("e"),
		[]byte("f"),
		[]byte("g"),
		[]byte("h"),
		[]byte("i"),
	}
	values := [][]byte{
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
		[]byte("5"),
		[]byte("6"),
		[]byte("7"),
		[]byte("8"),
		[]byte("9"),
	}

	// Insert keys and values into the SkipList
	for i, k := range keys {
		// Add time.Now() as the fourth argument to NewMemtableEntry
		entry := NewMemtableEntry(k, values[i], false, time.Now())
		s.Insert(entry)
	}

	// Print the SkipList
	fmt.Println("SkipList after insertion:")
	s.Print()

	// Search for keys in the SkipList
	for _, k := range keys {
		node := s.Get(k)
		if node != nil {
			fmt.Printf("Key %s found, value: %s\n", string(node.Key), string(node.Value))
		} else {
			fmt.Printf("Key %s not found\n", string(k))
		}
	}

	// Delete keys from the SkipList
	for _, k := range keys {
		s.Delete(k)
	}

	// Print the SkipList after deletion
	fmt.Println("SkipList after deletion:")
	s.Print()
}
*/
