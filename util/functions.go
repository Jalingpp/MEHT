package util

import (
	"encoding/hex"
	"fmt"
	"strconv"
)

func IntArrayToString(intArray []int) string {
	s := ""
	for _, v := range intArray {
		s += strconv.Itoa(v)
	}
	return s
}

func ByteToHexIndex(b byte) int {
	if b >= '0' && b <= '9' {
		return int(b - '0')
	}
	if b >= 'a' && b <= 'f' {
		return int(b - 'a' + 10)
	}
	return -1
}

func CommPrefix(a []byte, b []byte) []byte {
	var idx int
	for idx = 0; idx < len(a) && idx < len(b); idx++ {
		if a[idx] != b[idx] {
			break
		}
	}
	ret := make([]byte, idx)
	copy(ret, a)
	return ret
}

func StringToHex(s string) string {
	byteSlice := []byte(s)
	hexString := hex.EncodeToString(byteSlice)
	return hexString
}

func HexToString(hexString string) string {
	byteSlice, err := hex.DecodeString(hexString)
	if err != nil {
		fmt.Printf("HexToString error: %s\n", err)
		return ""
	}
	return string(byteSlice)
}

func Strip(input string, args string) string {
	s, args_ := []rune(input), []rune(args)
	lpos, rpos := 0, len(s)
	lok, rok := false, false
	argsMap := make(map[rune]bool)
	for i := 0; i < len(args_); i++ {
		argsMap[args_[i]] = true
	}
	for i := 0; i < len(s) && (!lok || !rok); i++ {
		if _, ok := argsMap[s[i]]; !ok {
			lok = true
			lpos = i
		}
		if _, ok := argsMap[s[len(s)-i-1]]; !ok {
			rok = true
			rpos = len(s) - i - 1
		}
	}
	if !lok || !rok {
		return ""
	}
	return string(s[lpos : rpos+1])
}
