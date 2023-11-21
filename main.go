package main

import (
	"MEHT/util"
	"sync"
	"time"

	"MEHT/sedb"

	// "encoding/hex"
	"fmt"
)

func main() {
	//测试SEDB
	//参数设置
	filePath := "data/levelDB/config.txt" //存储seHash和dbPath的文件路径
	//// siMode := "meht" //辅助索引类型，meht或mpt
	//siMode := "mpt"
	siMode := "meht"
	rdx := 16 //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	bc := 128 //meht中bucket的容量，即每个bucket中最多存储的KVPair数
	bs := 1   //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
	seHash, dbPath := sedb.ReadSEDBInfoFromFile(filePath)
	var seDB *sedb.SEDB
	//cacheEnable := false
	cacheEnable := true
	if cacheEnable {
		shortNodeCacheCapacity := 128
		fullNodeCacheCapacity := 128
		mgtNodeCacheCapacity := 100
		bucketCacheCapacity := 128
		segmentCacheCapacity := bs * bucketCacheCapacity
		merkleTreeCacheCapacity := bs * bucketCacheCapacity
		seDB = sedb.NewSEDB(seHash, dbPath, siMode, "test", rdx, bc, bs, cacheEnable,
			sedb.ShortNodeCacheCapacity(shortNodeCacheCapacity), sedb.FullNodeCacheCapacity(fullNodeCacheCapacity),
			sedb.MgtNodeCacheCapacity(mgtNodeCacheCapacity), sedb.BucketCacheCapacity(bucketCacheCapacity),
			sedb.SegmentCacheCapacity(segmentCacheCapacity), sedb.MerkleTreeCacheCapacity(merkleTreeCacheCapacity))
	} else {
		seDB = sedb.NewSEDB(seHash, dbPath, siMode, "test", rdx, bc, bs, cacheEnable)
	}
	//创建一个SEDB
	//读json文件创建KVPair数组
	kvdataPath := "data/OK"
	kvPairsJsonFiles, err := util.GetDirAllFilePathsFollowSymlink(kvdataPath)
	if err != nil {
		panic(err)
	}
	insertNum := 0
	var start time.Time
	var duration time.Duration = 0
	kvPairCh := make(chan *util.KVPair)

	allocate := func(kvPairsJsonFiles []string) {
		for j, file := range kvPairsJsonFiles {
			fmt.Println(file)
			kvPairs := util.ReadKVPairFromJsonFile(file)
			insertNum += len(kvPairs)
			//插入KVPair数组
			for i := 0; i < len(kvPairs); i++ {
				kvPairs[i].SetKey(util.StringToHex(kvPairs[i].GetKey()))
				kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
				//插入SEDB
				kvPairCh <- kvPairs[i]
				//seDB.InsertKVPair(kvPairs[i])
			}
			if j == 2 {
				break
			}
		}
		close(kvPairCh)
	}
	worker := func(wg *sync.WaitGroup) {
		for kvPair := range kvPairCh {
			seDB.InsertKVPair(kvPair)
		}
		wg.Done()
	}
	createWorkerPool := func(numOfWorker int) {
		var wg sync.WaitGroup
		for i := 0; i < numOfWorker; i++ {
			wg.Add(1)
			go worker(&wg)
		}
		wg.Wait()
	}
	numOfWorker := 2
	go allocate(kvPairsJsonFiles)
	start = time.Now()
	createWorkerPool(numOfWorker)
	duration = time.Since(start)

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
