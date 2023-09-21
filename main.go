package main

import (
	"MEHT/meht"
	"MEHT/util"
	"fmt"
)

func main() {

	//测试Bucket
	//创建一个bucket
	bucket := meht.NewBucket(0, 2, 2, 1) //ld,rdx,capacity,segNum
	//创建4个KVPair
	kvpair1 := util.NewKVPair("0000", "value1")
	kvpair2 := util.NewKVPair("0001", "value2")
	kvpair3 := util.NewKVPair("0010", "value3")
	kvpair4 := util.NewKVPair("0011", "value4")
	//插入4个KVPair
	buckets1 := bucket.Insert(*kvpair1)
	buckets2 := bucket.Insert(*kvpair2)
	buckets3 := bucket.Insert(*kvpair3)
	buckets4 := bucket.Insert(*kvpair4)

	//打印buckets1中所有的bucket
	fmt.Printf("buckets1中所有的bucket\n")
	for _, bucket := range buckets1 {
		bucket.PrintBucket()
	}

	//打印buckets2中所有的bucket
	fmt.Printf("buckets2中所有的bucket\n")
	for _, bucket := range buckets2 {
		bucket.PrintBucket()
	}

	//打印buckets3中所有的bucket
	fmt.Printf("buckets3中所有的bucket\n")
	for _, bucket := range buckets3 {
		bucket.PrintBucket()
	}

	//打印buckets4中所有的bucket
	fmt.Printf("buckets4中所有的bucket\n")
	for _, bucket := range buckets4 {
		bucket.PrintBucket()
	}
}
