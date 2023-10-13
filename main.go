package main

import (
	"MEHT/sedb"
	"MEHT/util"
	"encoding/hex"
	"fmt"
	// "github.com/syndtr/goleveldb/leveldb"
)

func main() {
	//测试SEDB

	//参数设置
	filePath := "data/levelDB/config.txt" //存储seHash和dbPath的文件路径
	// siMode := "meht" //辅助索引类型，meht或mpt
	siMode := "mpt"
	rdx := 16 //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	bc := 4   //meht中bucket的容量，即每个bucket中最多存储的KVPair数
	bs := 1   //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
	seHash, dbPath := sedb.ReadSEDBInfoFromFile(filePath)

	fmt.Printf("seHash:%s\n", hex.EncodeToString(seHash))

	//创建一个SEDB
	seDB := sedb.NewSEDB(seHash, dbPath, siMode, rdx, bc, bs)

	// //打印SEDB
	seDB.PrintSEDB()

	//// 测试插入不同长度的key
	//key1 := "00"
	//value1 := util.StringToHex("value1")
	//key2 := "000"
	//value2 := util.StringToHex("value2")
	//key3 := "012345678"
	//value3 := util.StringToHex("value3")
	//// 插入到SEDB中
	//seDB.InsertKVPair(util.NewKVPair(key1, value1))
	//seDB.InsertKVPair(util.NewKVPair(key2, value2))
	//seDB.InsertKVPair(util.NewKVPair(key3, value3))

	// 读文件创建一个KVPair数组
	//kvdataPath := "data/testdata.txt"
	//kvPairs := util.ReadKVPairFromFile(kvdataPath)
	kvdataPath := "data/testdata3.json"
	kvPairs := util.ReadKVPairFromJsonFile(kvdataPath)

	//插入KVPair数组
	for i := 1; i < len(kvPairs); i++ {
		kvPairs[i].SetKey(util.StringToHex(kvPairs[i].GetKey()))
		kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
		//插入SEDB
		seDB.InsertKVPair(kvPairs[i])
	}

	//打印SEDB
	seDB.PrintSEDB()

	//测试查询功能
	qkey := util.StringToHex("6071")
	qvalue, qresult, qproof := seDB.QueryKVPairsByHexKeyword(qkey)
	seDB.PrintKVPairsQueryResult(qkey, qvalue, qresult, qproof)
	//验证查询结果
	seDB.VerifyQueryResult(qvalue, qresult, qproof)

	//写seHash和dbPath到文件
	seDB.WriteSEDBInfoToFile(filePath)

}
