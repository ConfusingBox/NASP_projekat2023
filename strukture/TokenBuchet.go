package strukture

import (
	"encoding/binary"
	"time"
)

type TokenBucket struct {
	rate       int       // Tokens added per second
	bucketSize int       // Maximum tokens in the bucket
	tokens     int       // Current number of tokens
	lastRefill time.Time // Last time the bucket was refilled
}

func NewTokenBucket(rate int, bucketSize int) *TokenBucket {
	return &TokenBucket{
		rate:       rate,
		bucketSize: bucketSize,
		tokens:     bucketSize,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) Allow() bool {
	now := time.Now()
	tb.refill(now)

	if tb.tokens > 0 {
		tb.tokens--
		return true
	}

	return false
}

func (tb *TokenBucket) refill(now time.Time) {
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += int(elapsed * float64(tb.rate))

	if tb.tokens > tb.bucketSize {
		tb.tokens = tb.bucketSize
	}

	tb.lastRefill = now
}

func (tb *TokenBucket) Serialize() []byte {
	buf := make([]byte, 4+4+4+8)

	binary.BigEndian.PutUint32(buf[0:4], uint32(tb.rate))
	binary.BigEndian.PutUint32(buf[4:8], uint32(tb.bucketSize))
	binary.BigEndian.PutUint32(buf[8:12], uint32(tb.tokens))
	binary.BigEndian.PutUint64(buf[12:20], uint64(tb.lastRefill.Unix()))

	return buf
}

func Deserialize(data []byte) *TokenBucket {
	rate := int(binary.BigEndian.Uint32(data[0:4]))
	bucketSize := int(binary.BigEndian.Uint32(data[4:8]))
	tokens := int(binary.BigEndian.Uint32(data[8:12]))
	lastRefillUnix := int64(binary.BigEndian.Uint64(data[12:20]))
	lastRefill := time.Unix(lastRefillUnix, 0)

	return &TokenBucket{
		rate:       rate,
		bucketSize: bucketSize,
		tokens:     tokens,
		lastRefill: lastRefill,
	}
}
