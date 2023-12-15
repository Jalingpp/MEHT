package main

//
//import (
//	"MEHT/util"
//	"fmt"
//	"strconv"
//	"sync"
//	"time"
//
//	"MEHT/sedb"
//)
//
//func main() {
//	//测试SEDB
//	//参数设置
//	//allocateJsonFiles := func(kvPairsJsonFiles []string, kvPairCh chan *util.KVPair) (insertNum int) {
//	//	for j, file := range kvPairsJsonFiles {
//	//		fmt.Println(file)
//	//		kvPairs := util.ReadKVPairFromJsonFile(file)
//	//		insertNum += len(kvPairs)
//	//		//插入KVPair数组
//	//		for i := 0; i < len(kvPairs); i++ {
//	//			kvPairs[i].SetKey(util.StringToHex(kvPairs[i].GetKey()))
//	//			kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
//	//			//插入SEDB
//	//			kvPairCh <- kvPairs[i]
//	//			//seDB.InsertKVPair(kvPairs[i])
//	//		}
//	//		if j == 5 {
//	//			break
//	//		}
//	//	}
//	//	close(kvPairCh)
//	//	return
//	//}
//	allocateNFTOwner := func(filepath string, opNum int, kvPairCh chan *util.KVPair) {
//		kvPairs := util.ReadNFTOwnerFromFile(filepath, opNum)
//		for i := 0; i < min(opNum, len(kvPairs)); i++ {
//			kvPairs[i].SetKey(util.StringToHex(kvPairs[i].GetKey()))
//			kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
//			//插入SEDB
//			kvPairCh <- kvPairs[i]
//			//seDB.InsertKVPair(kvPairs[i])
//		}
//		close(kvPairCh)
//	}
//	//allocateQueryOwner := func(filepath string, opNum int) {
//	//	kvPairs := util.ReadNFTOwnerFromFile(filepath)
//	//	for i := 0; i < min(opNum, len(kvPairs)); i++ {
//	//		kvPairs[i].SetKey(util.StringToHex(kvPairs[i].GetKey()))
//	//		kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
//	//		//插入SEDB
//	//		kvPairCh <- kvPairs[i]
//	//		//seDB.InsertKVPair(kvPairs[i])
//	//	}
//	//	close(kvPairCh)
//	//}
//	worker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, kvPairCh chan *util.KVPair) {
//		for kvPair := range kvPairCh {
//			seDB.InsertKVPair(kvPair)
//		}
//		wg.Done()
//	}
//	createWorkerPool := func(numOfWorker int, seDB *sedb.SEDB, kvPairCh chan *util.KVPair) {
//		var wg sync.WaitGroup
//		for i := 0; i < numOfWorker; i++ {
//			wg.Add(1)
//			go worker(&wg, seDB, kvPairCh)
//		}
//		wg.Wait()
//	}
//	serializeArgs := func(siMode string, rdx int, bc int, bs int, cacheEnable bool,
//		shortNodeCC int, fullNodeCC int, mgtNodeCC int, bucketCC int, segmentCC int,
//		merkleTreeCC int, numOfWorker int) string {
//		return "siMode: " + siMode + ",\trdx: " + strconv.Itoa(rdx) + ",\tbc: " + strconv.Itoa(bc) +
//			",\tbs: " + strconv.Itoa(bs) + ",\tcacheEnable: " + strconv.FormatBool(cacheEnable) + ",\tshortNodeCacheCapacity: " +
//			strconv.Itoa(shortNodeCC) + ",\tfullNodeCacheCapacity: " + strconv.Itoa(fullNodeCC) + ",\tmgtNodeCacheCapacity" +
//			strconv.Itoa(mgtNodeCC) + ",\tbucketCacheCapacity: " + strconv.Itoa(bucketCC) + ",\tsegmentCacheCapacity: " +
//			strconv.Itoa(segmentCC) + ",\tmerkleTreeCacheCapacity: " + strconv.Itoa(merkleTreeCC) + ",\tnumOfThread: " +
//			strconv.Itoa(numOfWorker) + "."
//	}
//	var insertNum = []int{300000, 600000, 900000, 1200000, 1500000}
//	//var queryNum = []int{500000, 1000000, 1500000, 2000000, 2500000, 3000000}
//	for i, num := range insertNum {
//		if i == 0 {
//			continue
//		}
//		filePath := "data/levelDB/config.txt" //存储seHash和dbPath的文件路径
//		//siMode := "meht" //辅助索引类型，meht或mpt
//		//siMode := "mpt"
//		siMode := "meht"
//		rdx := 16  //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
//		bc := 1280 //meht中bucket的容量，即每个bucket中最多存储的KVPair数
//		bs := 1    //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
//		numOfWorker := 2
//		seHash, primaryDbPath, secondaryDbPath := sedb.ReadSEDBInfoFromFile(filePath)
//		var seDB *sedb.SEDB
//		//cacheEnable := false
//		cacheEnable := true
//		argsString := ""
//		if cacheEnable {
//			shortNodeCacheCapacity := 128000
//			fullNodeCacheCapacity := 128000
//			mgtNodeCacheCapacity := 100000
//			bucketCacheCapacity := 128000
//			segmentCacheCapacity := bs * bucketCacheCapacity
//			merkleTreeCacheCapacity := bs * bucketCacheCapacity
//			seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, "test", rdx, bc, bs, cacheEnable,
//				sedb.ShortNodeCacheCapacity(shortNodeCacheCapacity), sedb.FullNodeCacheCapacity(fullNodeCacheCapacity),
//				sedb.MgtNodeCacheCapacity(mgtNodeCacheCapacity), sedb.BucketCacheCapacity(bucketCacheCapacity),
//				sedb.SegmentCacheCapacity(segmentCacheCapacity), sedb.MerkleTreeCacheCapacity(merkleTreeCacheCapacity))
//			argsString = serializeArgs(siMode, rdx, bc, bs, cacheEnable, shortNodeCacheCapacity, fullNodeCacheCapacity, mgtNodeCacheCapacity, bucketCacheCapacity,
//				segmentCacheCapacity, merkleTreeCacheCapacity, numOfWorker)
//		} else {
//			seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, "test", rdx, bc, bs, cacheEnable)
//			argsString = serializeArgs(siMode, rdx, bc, bs, cacheEnable, 0, 0,
//				0, 0, 0, 0,
//				numOfWorker)
//		}
//		//创建一个SEDB
//		//读json文件创建KVPair数组
//		//kvdataPath := "data/OK"
//		//kvPairsJsonFiles, err := util.GetDirAllFilePathsFollowSymlink(kvdataPath)
//		//if err != nil {
//		//	panic(err)
//		//}
//		var start time.Time
//		var duration time.Duration = 0
//		kvPairCh := make(chan *util.KVPair)
//
//		//go allocateJsonFiles(kvPairsJsonFiles)
//		go allocateNFTOwner("data/nft-owner", num, kvPairCh)
//		start = time.Now()
//		createWorkerPool(numOfWorker, seDB, kvPairCh)
//		seDB.WriteSEDBInfoToFile(filePath)
//		duration = time.Since(start)
//		util.WriteResultToFile("data/result", argsString+"\tInsert "+strconv.Itoa(num)+" records in "+
//			duration.String()+", throughput = "+strconv.FormatFloat(float64(num)/duration.Seconds(), 'f', -1, 64)+" tps.")
//		fmt.Println("Insert ", num, " records in ", duration, ", throughput = ", float64(num)/duration.Seconds(), " tps.")
//		//os.RemoveAll("data/levelDB/testSEDB")
//		seDB = nil
//	}
//
//	//qrKey := "https://raw.seadn.io/files/e2205125604cfca54281e88783b4cd2b.gif,Human T1 [HATCHING],xs,human"
//	//qrValue, qrKVPair, qrProof := seDB.QueryKVPairsByHexKeyword(util.StringToHex(qrKey))
//	//seDB.PrintKVPairsQueryResult(qrKey, qrValue, qrKVPair, qrProof)
//	//打印SEDB
//	//seDB.PrintSEDB()
//
//	// // 测试插入不同长度的key
//	// key1 := "1020"
//	// value1 := util.StringToHex("Alice")
//	// key2 := "3021"
//	// value2 := util.StringToHex("Bob")
//	// key3 := "012345678"
//	// value3 := util.StringToHex("value5")
//	// // 插入到SEDB中
//	// seDB.InsertKVPair(util.NewKVPair(key1, value1))
//	// seDB.InsertKVPair(util.NewKVPair(key2, value2))
//	// seDB.InsertKVPair(util.NewKVPair(key3, value3))
//
//	// // 读文件创建一个KVPair数组
//	// kvdataPath := "data/testdata.txt"
//	// // kvdataPath := "C://Users//13219//Desktop//Data//NFT-ETH//nft-owner"
//	// // kvdataPath := "G://Data//NFT-ETH//nft-owner"
//	// kvPairs := util.ReadKVPairFromFile(kvdataPath)
//
//	// startTime := time.Now()
//
//	// //插入KVPair数组
//	// for i := 0; i < 10; i++ {
//	// 	//把KV转化为十六进制
//	// 	kvPairs[i].SetKey(kvPairs[i].GetKey())
//	// 	kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
//	// 	//插入SEDB
//	// 	seDB.InsertKVPair(kvPairs[i])
//	// 	// // 打印SEDB
//	// 	// seDB.PrintSEDB()
//	// 	// fmt.Println("Inserted i = ", i)
//	// 	// if i%100 == 0 {
//	// 	// 	fmt.Println("Inserted i = ", i)
//	// 	// }
//	// }
//
//	// endTime := time.Now()
//	// elapsedTime := endTime.Sub(startTime)
//	// // 打印运行时间
//	// fmt.Printf("插入运行时间: %s\n", elapsedTime)
//
//	// //打印SEDB中SEH的GD
//	// fmt.Println("GD = ", seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetSEH(seDB.GetDB()).GetGD())
//
//	// // 打印SEDB
//	// seDB.PrintSEDB()
//
//	//测试查询功能
//	//startTime_q := time.Now()
//	//for i := 0; i < 400; i++ {
//	//	qkey := util.StringToHex("Bob")
//	//	// qvalue, qresult, qproof := seDB.QueryKVPairsByHexKeyword(qkey)
//	//	qvalue, qresult, qproof := seDB.QueryKVPairsByHexKeyword(qkey)
//	//	// seDB.PrintKVPairsQueryResult(qkey, qvalue, qresult, qproof)
//	//	//验证查询结果
//	//	seDB.VerifyQueryResult(qvalue, qresult, qproof)
//	//}
//	//endTime_q := time.Now()
//	//elapsedTime_q := endTime_q.Sub(startTime_q)
//	//// 打印运行时间
//	//fmt.Printf("查询运行时间: %s\n", elapsedTime_q)
//	//
//	//// //测试查询功能
//	//// qkey2 := util.StringToHex("value6")
//	//// qvalue2, qresult2, qproof2 := seDB.QueryKVPairsByHexKeyword(qkey2)
//	//// seDB.PrintKVPairsQueryResult(qkey2, qvalue2, qresult2, qproof2)
//	//// //验证查询结果
//	//// seDB.VerifyQueryResult(qvalue2, qresult2, qproof2)
//	//
//	////打印访问频次列表和总访问次数
//	//seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetMGT(seDB.GetDB()).PrintHotnessList()
//	////打印桶的总数
//	//bucketNum := seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetSEH(seDB.GetDB()).GetBucketsNumber()
//	//fmt.Println("bucketNum =", bucketNum)
//	////打印总访问路径长度
//	//accessLength := seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetMGT(seDB.GetDB()).GetAccessLength()
//	//fmt.Println("accessLength =", accessLength)
//	////打印当前缓存情况
//	//seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetMGT(seDB.GetDB()).PrintCachedMaps()
//	////判断是否需要缓存调整
//	//IsNeedCacheAdjust := seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetMGT(seDB.GetDB()).IsNeedCacheAdjust(bucketNum, 0.9, 0.1)
//	//fmt.Println("是否需要调整缓存?", IsNeedCacheAdjust)
//	//
//	//// 进行负载调整
//	//startTime_a := time.Now()
//	//if IsNeedCacheAdjust {
//	//	seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).MGTCachedAdjust(seDB.GetDB())
//	//}
//	//endTime_a := time.Now()
//	//elapsedTime_a := endTime_a.Sub(startTime_a)
//	//// 打印运行时间
//	//fmt.Printf("调整运行时间: %s\n", elapsedTime_a)
//	//
//	////打印当前缓存情况
//	//seDB.GetStorageEngine().GetSecondaryIndex_meht(seDB.GetDB()).GetMGT(seDB.GetDB()).PrintCachedMaps()
//	//
//	////写seHash和dbPath到文件
//	//seDB.WriteSEDBInfoToFile(filePath)
//
//}
