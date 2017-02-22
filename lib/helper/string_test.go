package helper

import (
	"testing"
)

func TestSubstr(t *testing.T) {
	if Substr("foo bar", 0, 3) != "foo" {
		t.Errorf("Unexpected result")
	}

	if Substr("foo bar", 4, 3) != "bar" {
		t.Errorf("Unexpected result")
	}
}

func TestReverseString(t *testing.T) {
	if ReverseString("foo bar") != "rab oof" {
		t.Errorf("Unexpected result")
	}
}
