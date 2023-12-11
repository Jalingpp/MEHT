package util

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func HexIntToString(num int) string {
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
		return HexIntToString(num/16) + string(rune('a'+cur-10))
	} else {
		return HexIntToString(num/16) + strconv.Itoa(cur)
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
		cur := HexIntToString(val)
		ret += strings.Repeat("0", power-len(cur)) + cur
	}
	return ret
}

func StringToIntArray(str string, rdx int) ([]int, error) {
	// 计算进制的幂
	originalRdx := rdx
	power := 0
	for originalRdx >= 16 {
		if originalRdx%16 == 0 {
			power += 1
			originalRdx /= 16
		} else {
			return nil, fmt.Errorf("Invalid radix for hexadecimal conversion")
		}
	}

	// 确保字符串长度是进制的倍数
	if len(str)%power != 0 {
		return nil, fmt.Errorf("Invalid string length for the given radix")
	}

	// 分割字符串为进制长度的片段
	chunkSize := len(str) / power
	chunks := make([]string, chunkSize)
	for i := 0; i < chunkSize; i++ {
		chunks[i] = str[i*power : (i+1)*power]
	}

	// 转换每个片段为整数
	var result []int
	for _, chunk := range chunks {
		val, err := strconv.ParseInt(chunk, rdx, 64)
		if err != nil {
			fmt.Println("Error parsing chunk:", err)
			return nil, err
		}
		result = append(result, int(val))
	}
	return result, nil
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

type KV struct {
	key   string
	value int
}

func (kv *KV) PrintKV() {
	fmt.Println("key = ", kv.key, ", value = ", kv.value)
}

// 对给定的map[string]int根据int排序
func SortStringIntMapByInt(originmap *map[string]int) []*KV {
	var mapSlice []*KV
	for key, value := range *originmap {
		mapSlice = append(mapSlice, &KV{key, value})
	}
	sort.Slice(mapSlice, func(i, j int) bool {
		return mapSlice[i].value > mapSlice[j].value
	})
	return mapSlice
}
