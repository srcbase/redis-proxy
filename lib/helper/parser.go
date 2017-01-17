package helper

import (
	"regexp"
)

func ParseCommandKey(command string) string {
	re := regexp.MustCompile("\\w+")
	matchs := re.FindAllStringSubmatch(command, -1)
	if len(matchs) > 1 {
		return matchs[1][0]
	}

	return ""
}
