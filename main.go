package main

import (
	"MEHT/sedb"
	"MEHT/util"
	"fmt"
	"time"
	// "github.com/syndtr/goleveldb/leveldb"
)

func main() {
	//测试SEDB

	//参数设置
	filePath := "data/levelDB/config.txt" //存储seHash和dbPath的文件路径
	//// siMode := "meht" //辅助索引类型，meht或mpt
	siMode := "mpt"
	rdx := 16 //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	bc := 4   //meht中bucket的容量，即每个bucket中最多存储的KVPair数
	bs := 1   //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
	seHash, dbPath := sedb.ReadSEDBInfoFromFile(filePath)

	//fmt.Printf("seHash:%s\n", hex.EncodeToString(seHash))

	//创建一个SEDB
	seDB := sedb.NewSEDB(seHash, dbPath, siMode, rdx, bc, bs)

	// //打印SEDB
	//seDB.PrintSEDB()

	// 读txt文件创建一个KVPair数组
	//kvdataPath := "data/testdata2.txt"
	//kvPairs := util.ReadKVPairFromFile(kvdataPath)
	//for i := 0; i < len(kvPairs); i++ {
	//	kvPairs[i].SetKey(kvPairs[i].GetKey())
	//	kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
	//	seDB.InsertKVPair(kvPairs[i])
	//}

	//读json文件创建KVPair数组
	kvdataPath := "data/OK"
	kvPairsJsonFiles, err := util.GetDirAllFilePathsFollowSymlink(kvdataPath)
	if err != nil {
		panic(err)
	}
	insertNum := 0
	var start time.Time
	var duration time.Duration = 0
	for _, file := range kvPairsJsonFiles {
		fmt.Println(file)
		kvPairs := util.ReadKVPairFromJsonFile(file)
		insertNum += len(kvPairs)
		start = time.Now()
		//插入KVPair数组
		for i := 0; i < len(kvPairs); i++ {
			kvPairs[i].SetKey(util.StringToHex(kvPairs[i].GetKey()))
			kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
			//插入SEDB
			seDB.InsertKVPair(kvPairs[i])
		}
		duration += time.Since(start)
		start = time.Now()
		break
	}
	fmt.Println("Insert ", insertNum, " records in ", duration, ", throughput = ", float64(insertNum)/duration.Seconds(), " tps.")
	//打印SEDB
	//seDB.PrintSEDB()

	//测试查询功能
	//qkey := util.StringToHex("Alice")
	//qvalue, qresult, qproof := seDB.QueryKVPairsByHexKeyword(qkey)
	//seDB.PrintKVPairsQueryResult(qkey, qvalue, qresult, qproof)
	////验证查询结果
	//seDB.VerifyQueryResult(qvalue, qresult, qproof)
	//
	////写seHash和dbPath到文件
	seDB.WriteSEDBInfoToFile(filePath)

}
