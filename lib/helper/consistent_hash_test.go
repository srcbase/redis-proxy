package helper

import (
	"testing"
)

func TestMhash(t *testing.T) {
	if Mhash("foo bar") != 1781341601 {
		t.Errorf("Unexpected result")
	}
}
