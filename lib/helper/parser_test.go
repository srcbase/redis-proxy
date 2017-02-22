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

func TestParseCommandKey2(t *testing.T) {
	if ParseCommandKey2("*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n") != "foo" {
		t.Errorf("Unexpected result")
	}
}
