package main

import (
	"MEHT/db"
	"MEHT/util"
	"fmt"
	"strings"
)

//说明：每一个读进来的kv对都是一个KVPair，包含key和value，key和value都是string类型。
//需要先将key和value转化为十六进制，再插入StorageEngine中。在StorageEngine内部，key和value都是[]byte类型。
//对于查询得到的结果，直接将key由十六进制转化为字符串，value需要split后，再由十六进制转化为字符串。

func main() {
	//测试StorageEngine

	//参数设置
	// siMode := "meht" //辅助索引类型，meht或mpt
	siMode := "mpt"
	rdx := 16 //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	bc := 4   //meht中bucket的容量，即每个bucket中最多存储的KVPair数
	bs := 1   //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment

	//创建一个StorageEngine
	storageEngine := db.NewStorageEngine(siMode, rdx, bc, bs)

	//读文件创建一个KVPair数组
	kvPairs := util.ReadKVPairFromFile("data/testdata.txt")

	//插入KVPair数组
	for i := 0; i < len(kvPairs); i++ {
		//把KV转化为十六进制
		kvPairs[i].SetKey(util.StringToHex(kvPairs[i].GetKey()))
		kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
		//插入StorageEngine
		storageEngine.Insert(kvPairs[i])
	}

	// fmt.Printf("len(kvPairs)=%d\n", len(kvPairs))

	// //打印主索引
	// fmt.Printf("打印主索引-------------------------------------------------------------------------------------------\n")
	// storageEngine.GetPrimaryIndex().PrintMPT()
	// //打印辅助索引
	// fmt.Printf("打印辅助索引-------------------------------------------------------------------------------------------\n")
	// storageEngine.GetSecondaryIndex_mpt().PrintMPT()
	// storageEngine.GetSecondaryIndex_meht().PrintMEHT()

	// //测试查询（MEHT）
	// fmt.Printf("测试查询-------------------------------------------------------------------------------------------\n")
	// qk_meht := "Bob"
	// qv_meht, qvProof_meht := storageEngine.GetSecondaryIndex_meht().QueryByKey(util.StringToHex(qk_meht)) //需要将qk先转换为十六进制
	// qvs_meht := strings.Split(qv_meht, ",")                                                               //将查询结果qv按逗号分隔
	// fmt.Printf("key=%s查询结果：\n", qk_meht)
	// for i := 0; i < len(qvs_meht); i++ {
	// 	fmt.Printf("value=%s\n", util.HexToString(qvs_meht[i])) //将分裂后的查询结果转换为字符串输出
	// }
	// //打印查询结果（MEHT）
	// meht.PrintQueryResult(qk_meht, qv_meht, qvProof_meht)
	// //验证查询结果（MEHT）
	// // storageEngine.GetSecondaryIndex_mpt().VerifyQueryResult(qvBob, qvBobProof)
	// meht.VerifyQueryResult(qv_meht, qvProof_meht)

	//测试查询（MPT）
	fmt.Printf("测试查询-------------------------------------------------------------------------------------------\n")
	qk_mpt := "Alice"
	qv_mpt, qvProof_mpt := storageEngine.GetSecondaryIndex_mpt().QueryByKey(util.StringToHex(qk_mpt)) //需要将qk先转换为十六进制
	qvs_mpt := strings.Split(qv_mpt, ",")                                                             //将查询结果qv按逗号分隔
	fmt.Printf("key=%s查询结果：\n", qk_mpt)
	for i := 0; i < len(qvs_mpt); i++ {
		fmt.Printf("value=%s\n", util.HexToString(qvs_mpt[i])) //将分裂后的查询结果转换为字符串输出
	}
	//打印查询结果（MPT）
	storageEngine.GetSecondaryIndex_mpt().PrintQueryResult(qk_mpt, qv_mpt, qvProof_mpt)
	//验证查询结果（MPT）
	storageEngine.GetSecondaryIndex_mpt().VerifyQueryResult(qv_mpt, qvProof_mpt)

}
