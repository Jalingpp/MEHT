package main

import (
	"bufio"
	"fmt"
	"github.com/bits-and-blooms/bloom/v3"
	"os"
	"testing"
)

func TestPlayground(t *testing.T) {
	filter := bloom.NewWithEstimates(1000000, 0.01)
	fmt.Println(filter.BitSet().Len())
	f, _ := os.Create("myfile")
	w := bufio.NewWriter(f)
	byteToWrite, _ := filter.GobEncode()
	_, err := w.Write(byteToWrite)
	if err != nil {
		return
	}

}

func Test2Split(t *testing.T) {
	s := []int{0, 1, 2, 3, 4, 5, 6}
	var twoSplitFunc func(key int, l int, r int) int
	twoSplitFunc = func(key int, l int, r int) int {
		if l >= r {
			return l
		}
		mid := (r-l)/2 + l
		if s[mid] == key {
			return mid
		} else if s[mid] < key {
			return twoSplitFunc(key, mid+1, r)
		} else {
			return twoSplitFunc(key, l, mid-1)
		}
	}
	for i := 0; i < 7; i++ {
		fmt.Println(i, ": ", twoSplitFunc(i, 0, 7))
	}
}

func TestGetBucketByKeyWithBFFoo(t *testing.T) {
	var foo func(string, int, int) int
	f := bloom.NewWithEstimates(100000, 0.01)
	f.Add([]byte("h"))
	f.Add([]byte("he"))
	f.Add([]byte("hel"))
	//f.Add([]byte("l"))
	//f.Add([]byte("o"))

	foo = func(key string, l int, r int) int {
		if l >= r {
			return l
		} else if f.Test([]byte(key[:r+1])) {
			return r
		}
		mid := (r-l)/2 + l
		if f.Test([]byte(key[:mid+1])) {
			return foo(key, mid, r)
		} else {
			return foo(key, l, mid-1)
		}
	}
	fmt.Println(foo("hello", 0, 4))
}
