package helper

import (
	"testing"
)

func TestParseCommandKey(t *testing.T) {
	if ParseCommandKey("set foo bar") != "foo" {
		t.Errorf("Unexpected result")
	}

	if ParseCommandKey("get foo") != "foo" {
		t.Errorf("Unexpected result")
	}

	if ParseCommandKey("INFO") != "" {
		t.Errorf("Unexpected result")
	}

	if ParseCommandKey("lrange foo 1 100") != "foo" {
		t.Errorf("Unexpected result")
	}
}
