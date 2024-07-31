package utils

import (
	"strings"
)

var swear_word = map[string]struct{}{
	"баран": {},
	"дурак": {},
	"идиот": {},
}

func CensorWord(str string) string {

	var newSlice []string

	if len(str) <= 0 {
		return ""
	}

	strSlice := strings.Fields(str)

	for position, word := range strSlice {

		if _, ok := swear_word[strings.ToLower(word)]; ok {

			result := []rune(word)
			first_letter := string(result[0:1])
			replacement := strings.Repeat("#", len(result))
			replacement = strings.Replace(replacement, "#", first_letter, 1)
			strSlice[position] = replacement
			newSlice = append(strSlice[:position], strSlice[position:]...)
		}

	}
	if len(newSlice) > 0 {
		return strings.Join(newSlice, " ")
	}
	return str

}
