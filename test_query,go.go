package main

import (
	"MEHT/util"
	"fmt"
	"strconv"
	"sync"
	"time"

	"MEHT/sedb"
)

func main() {
	//测试SEDB查询
	allocateQueryOwner := func(filepath string, opNum int, queryCh chan string) {
		queries := util.ReadQueryOwnerFromFile(filepath, opNum)
		for _, query := range queries {
			queryCh <- query
		}
		close(queryCh)
	}
	worker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, queryCh chan string) {
		for query := range queryCh {
			seDB.QueryKVPairsByHexKeyword(query)
		}
		wg.Done()
	}
	createWorkerPool := func(numOfWorker int, seDB *sedb.SEDB, queryCh chan string) {
		var wg sync.WaitGroup
		for i := 0; i < numOfWorker; i++ {
			wg.Add(1)
			go worker(&wg, seDB, queryCh)
		}
		wg.Wait()
	}
	serializeArgs := func(siMode string, rdx int, bc int, bs int, cacheEnable bool,
		shortNodeCC int, fullNodeCC int, mgtNodeCC int, bucketCC int, segmentCC int,
		merkleTreeCC int, numOfWorker int) string {
		return "siMode: " + siMode + ",\trdx: " + strconv.Itoa(rdx) + ",\tbc: " + strconv.Itoa(bc) +
			",\tbs: " + strconv.Itoa(bs) + ",\tcacheEnable: " + strconv.FormatBool(cacheEnable) + ",\tshortNodeCacheCapacity: " +
			strconv.Itoa(shortNodeCC) + ",\tfullNodeCacheCapacity: " + strconv.Itoa(fullNodeCC) + ",\tmgtNodeCacheCapacity" +
			strconv.Itoa(mgtNodeCC) + ",\tbucketCacheCapacity: " + strconv.Itoa(bucketCC) + ",\tsegmentCacheCapacity: " +
			strconv.Itoa(segmentCC) + ",\tmerkleTreeCacheCapacity: " + strconv.Itoa(merkleTreeCC) + ",\tnumOfThread: " +
			strconv.Itoa(numOfWorker) + "."
	}
	//var queryNum = []int{300000, 600000, 900000, 1200000, 1500000}
	var queryNum = []int{9000}
	for _, num := range queryNum {
		filePath := "data/levelDB/config" + strconv.Itoa(num) + ".txt" //存储seHash和dbPath的文件路径
		//siMode := "meht" //辅助索引类型，meht或mpt
		//siMode := "mpt"
		siMode := "meht"
		rdx := 16  //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
		bc := 1280 //meht中bucket的容量，即每个bucket中最多存储的KVPair数
		bs := 1    //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
		numOfWorker := 2
		seHash, primaryDbPath, secondaryDbPath := sedb.ReadSEDBInfoFromFile(filePath)
		primaryDbPath += strconv.Itoa(num)
		secondaryDbPath += strconv.Itoa(num)
		var seDB *sedb.SEDB
		//cacheEnable := false
		cacheEnable := true
		argsString := ""
		if cacheEnable {
			shortNodeCacheCapacity := 128000
			fullNodeCacheCapacity := 128000
			mgtNodeCacheCapacity := 100000
			bucketCacheCapacity := 128000
			segmentCacheCapacity := bs * bucketCacheCapacity
			merkleTreeCacheCapacity := bs * bucketCacheCapacity
			seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, "test", rdx, bc, bs, cacheEnable,
				sedb.ShortNodeCacheCapacity(shortNodeCacheCapacity), sedb.FullNodeCacheCapacity(fullNodeCacheCapacity),
				sedb.MgtNodeCacheCapacity(mgtNodeCacheCapacity), sedb.BucketCacheCapacity(bucketCacheCapacity),
				sedb.SegmentCacheCapacity(segmentCacheCapacity), sedb.MerkleTreeCacheCapacity(merkleTreeCacheCapacity))
			argsString = serializeArgs(siMode, rdx, bc, bs, cacheEnable, shortNodeCacheCapacity, fullNodeCacheCapacity, mgtNodeCacheCapacity, bucketCacheCapacity,
				segmentCacheCapacity, merkleTreeCacheCapacity, numOfWorker)
		} else {
			seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, "test", rdx, bc, bs, cacheEnable)
			argsString = serializeArgs(siMode, rdx, bc, bs, cacheEnable, 0, 0,
				0, 0, 0, 0,
				numOfWorker)
		}
		//创建一个SEDB
		//读json文件创建KVPair数组
		//kvdataPath := "data/OK"
		//kvPairsJsonFiles, err := util.GetDirAllFilePathsFollowSymlink(kvdataPath)
		//if err != nil {
		//	panic(err)
		//}
		var start time.Time
		var duration time.Duration = 0
		queryCh := make(chan string)

		//go allocateJsonFiles(kvPairsJsonFiles)
		go allocateQueryOwner("data/nft-owner", num, queryCh)
		start = time.Now()
		createWorkerPool(numOfWorker, seDB, queryCh)
		seDB.WriteSEDBInfoToFile(filePath)
		duration = time.Since(start)
		util.WriteResultToFile("data/qresult", argsString+"\tQuery "+strconv.Itoa(num)+" records in "+
			duration.String()+", throughput = "+strconv.FormatFloat(float64(num)/duration.Seconds(), 'f', -1, 64)+" tps.")
		fmt.Println("Query ", num, " records in ", duration, ", throughput = ", float64(num)/duration.Seconds(), " tps.")
		seDB = nil
	}

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

}
