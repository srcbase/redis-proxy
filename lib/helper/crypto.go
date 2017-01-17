package helper

import (
	"crypto/md5"
	"fmt"
	"io"
)

func Md5(key string) string {
	h := md5.New()
	io.WriteString(h, key)
	return fmt.Sprintf("%x", h.Sum(nil))
}
