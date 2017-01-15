package helper

import (
	"strings"
)

func Mhash(key string) int64 {
	hash_string := Substr(Md5(key), 0, 8)

	var seed int64
	seed = 31
	var hash int64
	hash = 0
	hash_arr := strings.Split(hash_string, "")
	for _, val := range hash_arr {
		hash = seed*hash + int64([]byte(val)[0])
	}

	return hash & 0x7FFFFFFF
}
