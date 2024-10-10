package main

import (
	"fmt"
	"math"
	"strconv"

	"github.com/willf/bloom"
)

func main() {
	m := uint(400000)
	k := uint(3)
	n := uint(65536)

	filter := bloom.New(m, k)
	for i := 0; i < int(n); i++ {
		filter.Add([]byte("element" + strconv.Itoa(i)))
	}

	falsePositiveRate := math.Pow(1.0-math.Exp(-float64(k)*float64(n)/float64(m)), float64(k))

	fmt.Printf("The false positive rate of the Bloom filter is: %f\n", falsePositiveRate)

	// 检查元素
	if filter.Test([]byte("element1")) {
		fmt.Println("element1 might exist.")
	} else {
		fmt.Println("element1 does not exist.")
	}

	if filter.Test([]byte("element200")) {
		fmt.Println("element2 might exist.")
	} else {
		fmt.Println("element2 does not exist.")
	}

	str := "helloabcde"
	i := 7 // 假设我们想要获取字符串的第6个字符（下标从0开始）

	// 将字符串转为rune切片
	if i < len(str) {
		fmt.Printf("字符串的第 %d 个字符是: '%c'\n", i, str[i])
	} else {
		fmt.Println("索引超出字符串长度")
	}
}
