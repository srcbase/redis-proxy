package helper

import (
	"testing"
)

func TestInStringArray(t *testing.T) {
	array := []string{"a", "b", "c"}
	needle := "a"
	if !InStringArray(needle, array) {
		t.Errorf("Unexpected result")
	}

	needle = "d"
	if InStringArray(needle, array) {
		t.Errorf("Unexpected result")
	}
}
