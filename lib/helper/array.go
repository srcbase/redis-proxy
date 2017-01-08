package helper

func InStringArray(needle string, array []string) bool {
	in_string_array := false
	for _, val := range array {
		if val == needle {
			in_string_array = true
			break
		}
	}
	return in_string_array
}
