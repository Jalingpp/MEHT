package util

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

func IntToHEXString(num int) string {
	if num < 0 {
		return ""
	}
	if num < 10 {
		return strconv.Itoa(num)
	} else if num < 16 {
		return string(rune('a' + num - 10))
	}
	cur := num % 16
	if cur > 9 {
		return IntToHEXString(num/16) + string(rune('a'+cur-10))
	} else {
		return IntToHEXString(num/16) + strconv.Itoa(cur)
	}
}

func IntArrayToString(intArray []int, rdx int) string {
	ret := ""
	power := 0
	for rdx >= 16 {
		power += 1
		rdx /= 16
	}
	for _, val := range intArray {
		cur := IntToHEXString(val)
		ret += strings.Repeat("0", power-len(cur)) + cur
	}
	return ret
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

func StringToBucketKeyIdxWithRdx(str string, offset int, rdx int) int {
	BASE_ := 16
	power := 0
	rdx_ := rdx
	ret := 0
	for rdx_ >= BASE_ {
		power++
		rdx_ /= BASE_
	}
	for i := power - 1; i >= 0; i-- {
		ret = BASE_*ret + ByteToHexIndex(str[max(0, len(str)-power*offset-i)])
	}
	return ret
}

func CommPrefix(a string, b string) string {
	var idx int
	for idx = 0; idx < len(a) && idx < len(b); idx++ {
		if a[idx] != b[idx] {
			break
		}
	}
	return a[:idx]
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
		if _, ok := argsMap[s[i]]; !ok && !lok {
			lok = true
			lpos = i
		}
		if _, ok := argsMap[s[len(s)-i-1]]; !ok && !rok {
			rok = true
			rpos = len(s) - i - 1
		}
	}
	if !lok || !rok {
		return ""
	}
	return string(s[lpos : rpos+1])
}
