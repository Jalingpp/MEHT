package main

import (
	"MEHT/sedb"
	"MEHT/util"
	"fmt"
	"time"
	// "encoding/hex"
)

func main() {
	// //测试SEDB

	// //参数设置
	// // filePath := "data/levelDB/testMPT/config.txt" //存储seHash和dbPath的文件路径
	// filePath := "data/levelDB/testNFTETH/config.txt" //存储seHash和dbPath的文件路径
	filePath := "data/levelDB/testMEHT/config.txt" //存储seHash和dbPath的文件路径
	// // siMode := "meht" //辅助索引类型，meht或mpt
	siMode := "meht"
	mehtName := "OwnerIndex" //meht的名字
	rdx := 16                //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	bc := 2                  //meht中bucket的容量，即每个bucket中最多存储的KVPair数
	bs := 1                  //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
	seHash, dbPath := sedb.ReadSEDBInfoFromFile(filePath)

	// fmt.Printf("seHash:%s\n", hex.EncodeToString(seHash))

	//创建一个SEDB
	seDB := sedb.NewSEDB(seHash, dbPath, siMode, mehtName, rdx, bc, bs)

	// 打印SEDB
	// seDB.PrintSEDB()

	// // 测试插入不同长度的key
	// key1 := "1020"
	// value1 := util.StringToHex("Alice")
	// key2 := "3021"
	// value2 := util.StringToHex("Bob")
	// key3 := "012345678"
	// value3 := util.StringToHex("value5")
	// // 插入到SEDB中
	// seDB.InsertKVPair(util.NewKVPair(key1, value1))
	// seDB.InsertKVPair(util.NewKVPair(key2, value2))
	// seDB.InsertKVPair(util.NewKVPair(key3, value3))

	// // 读文件创建一个KVPair数组
	// kvdataPath := "data/testdata.txt"
	// // kvdataPath := "C://Users//13219//Desktop//Data//NFT-ETH//nft-owner"
	// // kvdataPath := "G://Data//NFT-ETH//nft-owner"
	// kvPairs := util.ReadKVPairFromFile(kvdataPath)

	// startTime := time.Now()

	// //插入KVPair数组
	// for i := 0; i < 10; i++ {
	// 	//把KV转化为十六进制
	// 	kvPairs[i].SetKey(kvPairs[i].GetKey())
	// 	kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
	// 	//插入SEDB
	// 	seDB.InsertKVPair(kvPairs[i])
	// 	// // 打印SEDB
	// 	// seDB.PrintSEDB()
	// 	// fmt.Println("Inserted i = ", i)
	// 	// if i%100 == 0 {
	// 	// 	fmt.Println("Inserted i = ", i)
	// 	// }
	// }

	// endTime := time.Now()
	// elapsedTime := endTime.Sub(startTime)
	// // 打印运行时间
	// fmt.Printf("插入运行时间: %s\n", elapsedTime)

	// //打印SEDB中SEH的GD
	// fmt.Println("GD = ", seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetSEH(seDB.GetDB()).GetGD())

	// // 打印SEDB
	// seDB.PrintSEDB()

	//测试查询功能
	startTime_q := time.Now()
	for i := 0; i < 400; i++ {
		qkey := util.StringToHex("Bob")
		// qvalue, qresult, qproof := seDB.QueryKVPairsByHexKeyword(qkey)
		qvalue, qresult, qproof := seDB.QueryKVPairsByHexKeyword(qkey)
		// seDB.PrintKVPairsQueryResult(qkey, qvalue, qresult, qproof)
		//验证查询结果
		seDB.VerifyQueryResult(qvalue, qresult, qproof)
	}
	endTime_q := time.Now()
	elapsedTime_q := endTime_q.Sub(startTime_q)
	// 打印运行时间
	fmt.Printf("查询运行时间: %s\n", elapsedTime_q)

	// //测试查询功能
	// qkey2 := util.StringToHex("value6")
	// qvalue2, qresult2, qproof2 := seDB.QueryKVPairsByHexKeyword(qkey2)
	// seDB.PrintKVPairsQueryResult(qkey2, qvalue2, qresult2, qproof2)
	// //验证查询结果
	// seDB.VerifyQueryResult(qvalue2, qresult2, qproof2)

	//打印访问频次列表和总访问次数
	seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetMGT(seDB.GetDB()).PrintHotnessList()
	//打印桶的总数
	bucketNum := seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetSEH(seDB.GetDB()).GetBucketsNumber()
	fmt.Println("bucketNum =", bucketNum)
	//打印总访问路径长度
	accessLength := seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetMGT(seDB.GetDB()).GetAccessLength()
	fmt.Println("accessLength =", accessLength)
	//打印当前缓存情况
	seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetMGT(seDB.GetDB()).PrintCachedMaps()
	//判断是否需要缓存调整
	IsNeedCacheAdjust := seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetMGT(seDB.GetDB()).IsNeedCacheAdjust(bucketNum, 0.9, 0.1)
	fmt.Println("是否需要调整缓存?", IsNeedCacheAdjust)

	// 进行负载调整
	startTime_a := time.Now()
	if IsNeedCacheAdjust {
		seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).MGTCachedAdjust(seDB.GetDB())
	}
	endTime_a := time.Now()
	elapsedTime_a := endTime_a.Sub(startTime_a)
	// 打印运行时间
	fmt.Printf("调整运行时间: %s\n", elapsedTime_a)

	//打印当前缓存情况
	seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetMGT(seDB.GetDB()).PrintCachedMaps()

	//写seHash和dbPath到文件
	seDB.WriteSEDBInfoToFile(filePath)

}
