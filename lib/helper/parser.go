package helper

import (
	"regexp"
	"strings"
)

func ParseCommandKey(command string) string {
	re := regexp.MustCompile("\\w+")
	matchs := re.FindAllStringSubmatch(command, -1)
	if len(matchs) > 1 {
		return matchs[1][0]
	}

	return ""
}

func ParseCommandKey2(command string) string {
	if strings.Index(command, "*2") == 0 {
		command_arr := strings.Split(command, "\r\n")
		if len(command_arr) >= 5 {
			return command_arr[4]
		}
	}

	return ""
}
