package utils

import (
	"log"
	"os"
	"strings"
)

// SplitInTwo splits a string to two parts by a delimeter
func SplitInTwo(s, sep string) (string, string) {
	if !strings.Contains(s, sep) {
		log.Fatal(s, "does not contain", sep)
	}
	split := strings.Split(s, sep)
	return split[0], split[1]
}

// ToggleEnvVar sets key to a new value and returns its old value
func ToggleEnvVar(key, value string) string {
	oldValue := os.Getenv(key)
	os.Setenv(key, value)

	return oldValue
}
