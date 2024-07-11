package main

import (
	"fmt"
	"testing"
)

func TestPlayground(t *testing.T) {
	var a []byte
	b := []byte("hello")
	c := []byte("world")
	c = append(c, b...)
	fmt.Println(len(c))
	c = append(c, a...)
	fmt.Println(len(c))
	d := [32]byte{}
	c = append(c, d[:]...)
	fmt.Println(len(c))
}
