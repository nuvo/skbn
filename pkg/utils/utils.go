package utils

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// toggleAWSVars handles the use of heptio-authenticator-aws alongside kubectl
func toggleAWSVars(awsProfile, awsSdkLoadConfig string) string {
	oldAWSProfile := ToggleEnvVar("AWS_PROFILE", awsProfile)

	return oldAWSProfile
}

// ToggleEnvVar sets key to a new value and returns its old value
func ToggleEnvVar(key, value string) string {
	oldValue := os.Getenv(key)
	os.Setenv(key, value)

	return oldValue
}

// SplitInTwo splits a string to two parts by a delimeter
func SplitInTwo(s, sep string) (string, string) {
	if !strings.Contains(s, sep) {
		log.Fatal(s, "does not contain", sep)
	}
	split := strings.Split(s, sep)
	return split[0], split[1]
}

// CountDigits counts the digits of an integer
func CountDigits(i int) (count int) {
	for i != 0 {

		i /= 10
		count = count + 1
	}
	return count
}

// LeftPad2Len pads s with padStr to match overallLen
func LeftPad2Len(num, padNum, overallLen int) string {
	s := strconv.Itoa(num)
	padStr := strconv.Itoa(padNum)
	var padCountInt = 1 + ((overallLen - len(padStr)) / len(padStr))
	var retStr = strings.Repeat(padStr, padCountInt) + s
	return retStr[(len(retStr) - overallLen):]
}

// Sleep sleeps for an input number of seconds
func Sleep(seconds int) {
	time.Sleep(time.Duration(seconds) * time.Second)
}
