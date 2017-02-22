package helper

import (
	"testing"
)

func TestMd5(t *testing.T) {
	if Md5("foo bar") != "327b6f07435811239bc47e1544353273" {
		t.Errorf("Unexpected result")
	}
}
