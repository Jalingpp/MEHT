package main

import (
	"MEHT/util"
	"time"

	"MEHT/sedb"

	// "encoding/hex"
	"fmt"
)

func main() {
	// TODO 后续并发的逻辑应该是：
	// 节点粒度锁，然后阻塞但无人等候就加入等候区
	// 等候区已经有线程在等候且空闲位置大于等候区插入数量，则委托等候线程代为插入
	// 否则满了就重做，这样一定会让那个工作数量最大或者会把节点插满的线程先做
	// 后续分裂出新节点了，那么重做也能找到新插入的位置
	// 因此需要在叶子节点上添加一个等候位置，以及对应的一个待插入数据集互斥锁0
	// 线程发现仍有空余位置可以插入或者悲观等待锁没有获取到树根写锁(这里需要一个标识判断被委托线程是否已经获取到树根锁)时，
	// 获取待插入数据集写锁，然后检查是否有空余位置
	// 有则加入进去，然后修改空余位置数量，然后将后续工作委托给悲观等待的线程
	// 没有则重新寻找待插入为位置
	// 如果发现待插入数据为0，那么自己就是个需要悲观等待的线程，那么更新待插入数据以后开始盲等MGT树根锁释放
	// 悲观等待发现获取到树根后去获取委托锁，保证不再接受委托，然后更新

	//测试SEDB

	//参数设置
	filePath := "data/levelDB/config.txt" //存储seHash和dbPath的文件路径
	//// siMode := "meht" //辅助索引类型，meht或mpt
	//siMode := "mpt"
	siMode := "meht"
	rdx := 16 //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	bc := 128 //meht中bucket的容量，即每个bucket中最多存储的KVPair数
	bs := 1   //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
	//cacheEnable := false
	cacheEnable := true
	var cacheArgs []interface{}
	if cacheEnable {
		mgtNodeCacheCapacity := 100
		bucketCacheCapacity := 128
		segmentCacheCapacity := bs * bucketCacheCapacity
		merkleTreeCacheCapacity := bs * bucketCacheCapacity
		cacheArgs = append(cacheArgs, sedb.MgtNodeCacheCapacity(mgtNodeCacheCapacity),
			sedb.BucketCacheCapacity(bucketCacheCapacity), sedb.SegmentCacheCapacity(segmentCacheCapacity),
			sedb.MerkleTreeCacheCapacity(merkleTreeCacheCapacity))
	}
	seHash, dbPath := sedb.ReadSEDBInfoFromFile(filePath)

	//创建一个SEDB
	seDB := sedb.NewSEDB(seHash, dbPath, siMode, "test", rdx, bc, bs, cacheEnable, cacheArgs)

	//读json文件创建KVPair数组
	kvdataPath := "data/OK"
	kvPairsJsonFiles, err := util.GetDirAllFilePathsFollowSymlink(kvdataPath)
	if err != nil {
		panic(err)
	}
	insertNum := 0
	var start time.Time
	var duration time.Duration = 0
	//for _, file := range kvPairsJsonFiles {
	for j, file := range kvPairsJsonFiles {
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
		if j == 2 {
			break
		}
	}
	fmt.Println("Insert ", insertNum, " records in ", duration, ", throughput = ", float64(insertNum)/duration.Seconds(), " tps.")
	//qrKey := "https://raw.seadn.io/files/e2205125604cfca54281e88783b4cd2b.gif,Human T1 [HATCHING],xs,human"
	//qrValue, qrKVPair, qrProof := seDB.QueryKVPairsByHexKeyword(util.StringToHex(qrKey))
	//seDB.PrintKVPairsQueryResult(qrKey, qrValue, qrKVPair, qrProof)
	//打印SEDB
	//seDB.PrintSEDB()

	// 插入kvpair2到MEHT
	//MEHT.Insert(kvpair2, db)
	//// 打印整个MEHT
	//MEHT.PrintMEHT(db)
	//
	////插入kvpair3到MEHT
	//MEHT.Insert(kvpair3, db)
	////打印整个MEHT
	//MEHT.PrintMEHT(db)
	//
	////插入kvpair4到MEHT
	//MEHT.Insert(kvpair4, db)
	//// //打印整个MEHT
	//MEHT.PrintMEHT(db)
	//
	//MEHT.UpdateMEHTToDB(db)

	// //查询kvpair1
	// qv1, bucket1, segkey1, isSegExist1, index1 := MEHT.QueryValueByKey(kvpair1.GetKey(), db)
	// //获取查询证明
	// qpf1 := MEHT.GetQueryProof(bucket1, segkey1, isSegExist1, index1, db)
	// //打印查询结果
	// meht.PrintQueryResult(kvpair1.GetKey(), qv1, qpf1)
	// //验证查询结果
	// meht.VerifyQueryResult(qv1, qpf1)

	// //查询kvpair1
	// qv2, bucket2, segkey2, isSegExist2, index2 := MEHT.QueryValueByKey(kvpair2.GetKey(), db)
	// //获取查询证明
	// qpf2 := MEHT.GetQueryProof(bucket2, segkey2, isSegExist2, index2, db)
	// //打印查询结果
	// meht.PrintQueryResult(kvpair2.GetKey(), qv2, qpf2)
	// //验证查询结果
	// meht.VerifyQueryResult(qv2, qpf2)

	seDB.WriteSEDBInfoToFile(filePath)

}
