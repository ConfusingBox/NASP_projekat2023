package strukture

import (
	"container/list"
	"fmt"
)

// LRUCache represents a Least Recently Used (LRU) cache.
type LRUCache struct {
	capacity int64                    // The maximum number of elements the cache can hold.
	cache    map[string]*list.Element // A map for O(1) access to cache items.
	pages    *list.List               // A list to track the order of elements for eviction policy.
}

// Page represents a page that can be stored in the cache.
type Page struct {
	key   []byte // The key associated with the value.
	value []byte // The value to be stored.
}

// NewLRUCache creates a new LRUCache with the given capacity.
func NewLRUCache(capacity int64) LRUCache {
	return LRUCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		pages:    list.New(),
	}
}

// Get retrieves a value from the cache using its key.
// It returns nil if the key does not exist in the cache.
func (l *LRUCache) Get(key []byte) []byte {
	if element, ok := l.cache[string(key)]; ok {
		l.pages.MoveToFront(element) // Move accessed element to the front as it is the most recently used.
		return element.Value.(*Page).value
	}
	return nil
}

// Put inserts a key-value pair into the cache.
// If the key already exists, it updates the value and moves the key to the front.
// If the cache is full, it removes the least recently used item before inserting the new one.
func (l *LRUCache) Put(key []byte, value []byte) {
	if element, ok := l.cache[string(key)]; ok {
		l.pages.MoveToFront(element) // Move updated element to the front as it is the most recently used.
		element.Value.(*Page).value = value
	} else {
		if int64(l.pages.Len()) >= l.capacity {
			// Remove the least recently used element from the cache.
			delete(l.cache, string(l.pages.Back().Value.(*Page).key))
			l.pages.Remove(l.pages.Back())
		}
		// Add new element to the front of the list and to the cache.
		l.pages.PushFront(&Page{key, value})
		l.cache[string(key)] = l.pages.Front()
	}
}

func (l *LRUCache) Remove(key []byte) {
	if element, ok := l.cache[string(key)]; ok {
		l.pages.Remove(element)
		delete(l.cache, string(key))
	}
}

// Print prints the keys and values of the LRUCache.
func (l *LRUCache) Print() {
	// Start from the head of the list
	for node := l.pages.Front(); node != nil; node = node.Next() {
		// Get the page from the list node
		page := node.Value.(*Page)
		// Print the key and value of the page
		fmt.Printf("Key: %s, Value: %s\n", string(page.key), string(page.value))
	}
	fmt.Println()
}

// func main() {
// 	// Create a new LRUCache with capacity 3
// 	cache := NewLRUCache(3)

// 	// Insert key-value pairs into the cache
// 	cache.Put([]byte("a"), []byte("1"))
// 	cache.Put([]byte("b"), []byte("2"))
// 	cache.Put([]byte("c"), []byte("3"))

// 	// Print the cache
// 	fmt.Println("Cache after inserting a, b, c:")
// 	cache.Print()

// 	// Access one of the existing keys
// 	fmt.Printf("Accessing key a: %s\n", cache.Get([]byte("a")))

// 	// Print the cache after accessing a key
// 	fmt.Println("Cache after accessing key a:")
// 	cache.Print()

// 	// Insert a new key-value pair, this should evict the least recently used key
// 	cache.Put([]byte("d"), []byte("4"))

// 	// Print the cache after inserting a new key
// 	fmt.Println("Cache after inserting key d:")
// 	cache.Print()

// 	// Try to access a key that has been evicted
// 	fmt.Printf("Accessing evicted key b: %s\n", cache.Get([]byte("b")))
// }
