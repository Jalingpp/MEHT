package main

import (
	"MEHT/mpt"
	"MEHT/util"
)

func main() {
	//测试MPT insert
	mpt := mpt.NewMPT()

	kvpair1 := util.NewKVPair("a711355", "value1")
	kvpair2 := util.NewKVPair("a77d337", "value2")
	kvpair3 := util.NewKVPair("a7f9365", "value3")
	kvpair4 := util.NewKVPair("a77d397", "value4")

	//测试MPT insert
	mpt.Insert(*kvpair1)
	mpt.Insert(*kvpair2)
	mpt.Insert(*kvpair3)
	mpt.Insert(*kvpair4)

	// kvpair4 = util.NewKVPair("a77d397", "value5")

	// fmt.Printf("key1:%x\n", []byte(kvpair1.GetKey()))
	// fmt.Printf("value1:%x\n", []byte(kvpair1.GetValue()))
	// fmt.Printf("key2:%x\n", []byte(kvpair2.GetKey()))
	// fmt.Printf("value2:%x\n", []byte(kvpair2.GetValue()))
	// fmt.Printf("key3:%x\n", []byte(kvpair3.GetKey()))
	// fmt.Printf("value3:%x\n", []byte(kvpair3.GetValue()))
	// fmt.Printf("key4:%x\n", []byte(kvpair4.GetKey()))
	// fmt.Printf("value4:%x\n", []byte(kvpair4.GetValue()))
	// mpt.Insert(*kvpair4)

	//测试MPT print
	mpt.PrintMPT()

	//测试MPT query
	// qv1, qpf1 := mpt.QueryByKey(kvpair1.GetKey())
	// //打印查询结果
	// mpt.PrintQueryResult(kvpair1.GetKey(), qv1, qpf1)

	// //测试MPT query
	// qv2, qpf2 := mpt.QueryByKey(kvpair2.GetKey())
	// //打印查询结果
	// mpt.PrintQueryResult(kvpair2.GetKey(), qv2, qpf2)

	//测试MPT query
	// qv3, qpf3 := mpt.QueryByKey(kvpair3.GetKey())
	// //打印查询结果
	// mpt.PrintQueryResult(kvpair3.GetKey(), qv3, qpf3)

	//测试MPT query
	qverr, qpferr := mpt.QueryByKey("a77d344")
	//打印查询结果
	mpt.PrintQueryResult("a77d344", qverr, qpferr)

	// //测试MPT verify
	// mpt.VerifyQueryResult(qv1, qpf1)
	// mpt.VerifyQueryResult(qv2, qpf2)
	// mpt.VerifyQueryResult(qv3, qpf3)
	mpt.VerifyQueryResult(qverr, qpferr)

	// mpt.PrintMPT()
}
