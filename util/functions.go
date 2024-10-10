package util

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"reflect"
	"sort"
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
	power := ComputeStrideByBase(rdx)
	for _, val := range intArray {
		cur := IntToHEXString(val)
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
			return nil, fmt.Errorf("invalid radix for hexadecimal conversion")
		}
	}

	// 确保字符串长度是进制的倍数
	if len(str)%power != 0 {
		return nil, fmt.Errorf("invalid string length for the given radix")
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

func ComputeStrideByBase(rdx int) (power int) {
	BASE_ := 16
	if rdx < BASE_ {
		panic("Rdx is smaller than base 16.")
	}
	power = 1
	rdx_ := rdx
	for rdx_ > BASE_ {
		power++
		rdx_ /= BASE_
	}
	return
}

func StringToBucketKeyIdxWithRdx(str string, offset int, rdx int) int {
	BASE_ := 16
	ret := 0
	power := ComputeStrideByBase(rdx)
	keyPoint := len(str) - power*offset - (power - 1)
	var startPos = 0
	if keyPoint > 0 {
		startPos = keyPoint
	}
	doNum := power
	if keyPoint < 0 { // 如果起始位置小于0,则从0位开始但截取长度比原先少了起始位置与0的距离的长度
		doNum += keyPoint
	}
	for i := 0; i < doNum; i++ {
		ret = BASE_*ret + ByteToHexIndex(str[startPos+i])
	}
	return ret
}

func CommPrefix(a string, b string) string {
	idx := 0
	for ; idx < len(a) && idx < len(b); idx++ {
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
	lPos, rPos := 0, len(s)
	lok, rok := false, false
	argsMap := make(map[rune]bool)
	for i := 0; i < len(args_); i++ {
		argsMap[args_[i]] = true
	}
	for i := 0; i < len(s) && (!lok || !rok); i++ {
		if _, ok := argsMap[s[i]]; !ok && !lok {
			lok = true
			lPos = i
		}
		if _, ok := argsMap[s[len(s)-i-1]]; !ok && !rok {
			rok = true
			rPos = len(s) - i - 1
		}
	}
	if !lok || !rok {
		return ""
	}
	ret := make([]rune, rPos+1-lPos)
	copy(ret, s[lPos:rPos+1])
	return string(ret)
}

var (
	errorType       = reflect.TypeOf((*error)(nil)).Elem()
	fmtStringerType = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
)

func IndirectToStringerOrError(a any) any {
	if a == nil {
		return nil
	}
	v := reflect.ValueOf(a)
	for !v.Type().Implements(fmtStringerType) && !v.Type().Implements(errorType) && v.Kind() == reflect.Pointer && !v.IsNil() {
		v = v.Elem()
	}
	return v.Interface()
}

// ToStringE casts any type to a string type.
func ToStringE(i any) (string, error) {
	i = IndirectToStringerOrError(i)
	switch s := i.(type) {
	case string:
		return s, nil
	case bool:
		return strconv.FormatBool(s), nil
	case float64:
		return strconv.FormatFloat(s, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(s), 'f', -1, 32), nil
	case int:
		return strconv.Itoa(s), nil
	case int64:
		return strconv.FormatInt(s, 10), nil
	case int32:
		return strconv.Itoa(int(s)), nil
	case int16:
		return strconv.FormatInt(int64(s), 10), nil
	case int8:
		return strconv.FormatInt(int64(s), 10), nil
	case uint:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint64:
		return strconv.FormatUint(s, 10), nil
	case uint32:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(s), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(s), 10), nil
	case json.Number:
		return s.String(), nil
	case []byte:
		return string(s), nil
	case template.HTML:
		return string(s), nil
	case template.URL:
		return string(s), nil
	case template.JS:
		return string(s), nil
	case template.CSS:
		return string(s), nil
	case template.HTMLAttr:
		return string(s), nil
	case nil:
		return "", nil
	case fmt.Stringer:
		return s.String(), nil
	case error:
		return s.Error(), nil
	default:
		return "", fmt.Errorf("unable to cast %#v of type %T to string", i, i)
	}
}

type KV struct {
	key   string
	value int
}

func (kv *KV) PrintKV() {
	fmt.Println("key = ", kv.key, ", value = ", kv.value)
}

func (kv *KV) GetValue() int {
	return kv.value
}

func (kv *KV) GetKey() string {
	return kv.key
}

// SortStringIntMapByInt 对给定的map[string]int根据int排序
func SortStringIntMapByInt(originMap *map[string]int) []*KV {
	var mapSlice []*KV
	for key, value := range *originMap {
		mapSlice = append(mapSlice, &KV{key, value})
	}
	sort.Slice(mapSlice, func(i, j int) bool {
		return mapSlice[i].value > mapSlice[j].value
	})
	return mapSlice
}

func StringToBool(s string) bool {
	switch strings.ToLower(s) {
	case "true", "1", "t", "yes", "y":
		return true
	default:
		return false
	}
}
