package helper

import (
	"regexp"
	"strings"
)

const CRLF = "\r\n"

func ParseCommandKey(command string) string {
	re := regexp.MustCompile("\\w+")
	matchs := re.FindAllStringSubmatch(command, -1)
	if len(matchs) > 1 {
		return matchs[1][0]
	}

	return ""
}

func ParseCommandKey2(command string) string {
	command_arr := strings.Split(command, CRLF)
	if len(command_arr) >= 5 {
		return command_arr[4];
	}

	return ""
}
