package util

import (
	"math/rand"
	"sync"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var randomSource = rand.NewSource(time.Now().UnixNano())
var randomSourceMu = sync.Mutex{}

func RandomString(size int) string {
	randomSourceMu.Lock()
	defer randomSourceMu.Unlock()

	buffer := make([]byte, size)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for index, cache, remain := size-1, randomSource.Int63(), letterIdxMax; index >= 0; {
		if remain == 0 {
			cache, remain = randomSource.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			buffer[index] = letterBytes[idx]
			index--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(buffer)
}
