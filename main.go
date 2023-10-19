package main

import (
	"MEHT/meht"
	// "MEHT/sedb"
	"MEHT/util"
	// "encoding/hex"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	// //测试SEDB

	// //参数设置
	// // filePath := "data/levelDB/testMPT/config.txt" //存储seHash和dbPath的文件路径
	// filePath := "data/levelDB/testMEHT/config.txt" //存储seHash和dbPath的文件路径
	// // siMode := "meht" //辅助索引类型，meht或mpt
	// siMode := "meht"
	mehtName := "OwnerIndex" //meht的名字
	// rdx := 16                //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	// bc := 4                  //meht中bucket的容量，即每个bucket中最多存储的KVPair数
	// bs := 1                  //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
	// seHash, dbPath := sedb.ReadSEDBInfoFromFile(filePath)

	// fmt.Printf("seHash:%s\n", hex.EncodeToString(seHash))

	// //创建一个SEDB
	// seDB := sedb.NewSEDB(seHash, dbPath, siMode, mehtName, rdx, bc, bs)

	// // //打印SEDB
	// // seDB.PrintSEDB()

	// // 测试插入不同长度的key
	// // key1 := "1020"
	// // value1 := util.StringToHex("Alice")
	// // key2 := "3021"
	// // value2 := util.StringToHex("Bob")
	// // key3 := "012345678"
	// // value3 := util.StringToHex("value5")
	// // // 插入到SEDB中
	// // seDB.InsertKVPair(util.NewKVPair(key1, value1))
	// // seDB.InsertKVPair(util.NewKVPair(key2, value2))
	// // seDB.InsertKVPair(util.NewKVPair(key3, value3))

	// // 读文件创建一个KVPair数组
	// kvdataPath := "data/testdata.txt"
	// kvPairs := util.ReadKVPairFromFile(kvdataPath)

	// //插入KVPair数组
	// for i := 0; i < 6; i++ {
	// 	//把KV转化为十六进制
	// 	kvPairs[i].SetKey(kvPairs[i].GetKey())
	// 	kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
	// 	//插入SEDB
	// 	seDB.InsertKVPair(kvPairs[i])
	// }

	// //打印SEDB
	// seDB.PrintSEDB()

	// //测试查询功能
	// qkey := util.StringToHex("Cindy")
	// qvalue, qresult, qproof := seDB.QueryKVPairsByHexKeyword(qkey)
	// seDB.PrintKVPairsQueryResult(qkey, qvalue, qresult, qproof)
	// //验证查询结果
	// seDB.VerifyQueryResult(qvalue, qresult, qproof)

	// //测试查询功能
	// qkey2 := util.StringToHex("value6")
	// qvalue2, qresult2, qproof2 := seDB.QueryKVPairsByHexKeyword(qkey2)
	// seDB.PrintKVPairsQueryResult(qkey2, qvalue2, qresult2, qproof2)
	// //验证查询结果
	// seDB.VerifyQueryResult(qvalue2, qresult2, qproof2)

	// //写seHash和dbPath到文件
	// seDB.WriteSEDBInfoToFile(filePath)

	//new a database
	dbPath := "data/levelDB/test"
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		fmt.Printf("OpenDB error: %v\n", err)
		return
	}

	//测试Bucket
	//创建一个bucket
	bucket := meht.NewBucket(mehtName, 0, 2, 2, 1) //ld,rdx,capacity,segNum
	//创建4个KVPair
	kvpair1 := util.NewKVPair("1000", "value5")
	kvpair2 := util.NewKVPair("1001", "value6")
	kvpair3 := util.NewKVPair("1010", "value7")
	kvpair4 := util.NewKVPair("1000", "value8")

	//插入4个KVPair

	buckets1 := bucket.Insert(kvpair1, db)
	//打印buckets1中所有的bucket
	fmt.Printf("buckets1中所有的bucket\n")
	for _, bucket := range buckets1 {
		bucket.PrintBucket(db)
		bucket.UpdateBucketToDB(db)
	}

	buckets2 := bucket.Insert(kvpair2, db)
	//打印buckets2中所有的bucket
	fmt.Printf("buckets2中所有的bucket\n")
	for _, bucket := range buckets2 {
		bucket.PrintBucket(db)
		bucket.UpdateBucketToDB(db)
	}

	buckets3 := bucket.Insert(kvpair3, db)
	//打印buckets3中所有的bucket
	fmt.Printf("buckets3中所有的bucket\n")
	for _, bucket := range buckets3 {
		bucket.PrintBucket(db)
		bucket.UpdateBucketToDB(db)
	}

	buckets4 := bucket.Insert(kvpair4, db)
	//打印buckets4中所有的bucket
	fmt.Printf("buckets4中所有的bucket\n")
	for _, bucket := range buckets4 {
		bucket.PrintBucket(db)
		bucket.UpdateBucketToDB(db)
	}

}
