package util

import (
	"strconv"
)

func IntArrayToString(intArray []int) string {
	s := ""
	for _, v := range intArray {
		s += strconv.Itoa(v)
	}
	return s
}
