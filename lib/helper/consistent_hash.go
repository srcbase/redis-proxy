package helper

import (
	"strings"
)

var cache map[string]int64

func init() {
	cache = make(map[string]int64)
}

func Mhash(key string) int64 {
	cache_hash := getCache(key)
	if cache_hash != 0 {
		return cache_hash
	}

	hash_string := Substr(Md5(key), 0, 8)

	var seed int64
	seed = 31
	var hash int64
	hash = 0
	hash_arr := strings.Split(hash_string, "")
	for _, val := range hash_arr {
		hash = seed*hash + int64([]byte(val)[0])
	}

	result := hash & 0x7FFFFFFF

	setCache(key, result)

	return result
}

func getCache(key string) int64 {
	if hash, ok := cache[key]; ok {
		return hash
	}

	return 0
}

func setCache(key string, hash int64) {
	cache[key] = hash
}
