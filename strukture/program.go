package strukture

type Program struct {
	Wal         *WriteAheadLog
	LRUcache    *LRUCache
	simHash     *SimHash
	tokenBucket *TokenBucket
}
