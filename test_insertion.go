package main

import (
	"MEHT/util"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"MEHT/sedb"
)

type IntegerWithLock struct {
	number int
	lock   sync.Mutex
}

func main() {
	//测试辅助索引查询
	//allocateNFTOwner := func(filepath string, opNum int, kvPairCh chan util.KVPair, phi int) {
	//	// PHI 代表分割分位数
	//	kvPairs := util.ReadNFTOwnerFromFile(filepath, opNum)
	//	wG := sync.WaitGroup{}
	//	wG.Add(phi)
	//	batchNum := len(kvPairs)/phi + 1
	//	for i := 0; i < phi; i++ {
	//		idx := i
	//		go func() {
	//			st := idx * batchNum
	//			var ed int
	//			if idx != phi-1 {
	//				ed = (idx + 1) * batchNum
	//			} else {
	//				ed = len(kvPairs)
	//			}
	//			for _, kvPair := range kvPairs[st:ed] {
	//				kvPair.SetKey(util.StringToHex(kvPair.GetKey()))
	//				kvPair.SetValue(util.StringToHex(kvPair.GetValue()))
	//				kvPairCh <- kvPair
	//			}
	//			wG.Done()
	//		}()
	//	}
	//	wG.Wait()
	//	close(kvPairCh)
	//}
	var batchSize = 10000
	var batchTimeout float64 = 100
	var curStartNum = IntegerWithLock{0, sync.Mutex{}}
	var curFinishNum = IntegerWithLock{0, sync.Mutex{}}
	var stopBatchCommitterFlag = true
	//var a = 0.9
	//var b = 0.1
	allocateNFTOwner := func(filepath string, opNum int, kvPairCh chan util.KVPair) {
		// PHI 代表分割分位数
		kvPairs := util.ReadNFTOwnerFromFile(filepath, opNum)
		for i, kvPair := range kvPairs {
			kvPair.SetKey(util.StringToHex(kvPair.GetKey()))
			kvPair.SetValue(util.StringToHex(kvPair.GetValue()))
			kvPairCh <- kvPair
			fmt.Println(i)
		}
		close(kvPairCh)
	}
	batchCommitter := func(wG *sync.WaitGroup, seDB *sedb.SEDB) {
		sT := time.Now()
		for stopBatchCommitterFlag {
			for curStartNum.number >= batchSize || time.Since(sT).Seconds() >= batchTimeout {
				curStartNum.lock.Lock()                         //阻塞新插入或查询操作
				for curStartNum.number != curFinishNum.number { //等待所有旧插入或查询操作完成
				}
				// 批量提交，即一并更新辅助索引的脏数据
				seDB.BatchCommit()
				//seDB.CacheAdjust(a, b)
				// 重置计数器
				curFinishNum.number = 0
				curStartNum.number = 0
				curStartNum.lock.Unlock()
				sT = time.Now()
			}
		}
		// 所有查询与插入操作已全部结束，将尾部数据批量提交
		seDB.BatchCommit()
		//seDB.CacheAdjust(a, b)
		wG.Done()
	}
	worker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, kvPairCh chan util.KVPair, durationCh chan time.Duration) {
		for kvPair := range kvPairCh {
			for {
				if curStartNum.number < batchSize && curStartNum.lock.TryLock() {
					if curStartNum.number == batchSize {
						curStartNum.lock.Unlock()
						continue
					}
					curStartNum.number++
					curStartNum.lock.Unlock()
					break
				}
			}
			st := time.Now()
			seDB.InsertKVPair(kvPair, false)
			du := time.Since(st)
			curFinishNum.lock.Lock()
			curFinishNum.number++
			curFinishNum.lock.Unlock()
			durationCh <- du
		}
		wg.Done()
	}
	countLatency := func(rets *[]time.Duration, durationChList *[]chan time.Duration, done chan bool) {
		wG := sync.WaitGroup{}
		wG.Add(len(*rets))
		for i := 0; i < len(*rets); i++ {
			idx := i

			go func() {
				ch := (*durationChList)[idx]
				for du := range ch {
					(*rets)[idx] += du
				}
				wG.Done()
			}()
		}
		wG.Wait()
		done <- true
	}
	createWorkerPool := func(numOfWorker int, seDB *sedb.SEDB, kvPairCh chan util.KVPair, durationChList *[]chan time.Duration) {
		var wg sync.WaitGroup
		for i := 0; i < numOfWorker; i++ {
			wg.Add(1)
			go worker(&wg, seDB, kvPairCh, (*durationChList)[i])
		}
		wg.Wait()
		stopBatchCommitterFlag = false
		for _, duCh := range *durationChList {
			close(duCh)
		}
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

	var insertNum = make([]int, 0)
	var siModeOptions = make([]string, 0)
	var numOfWorker = 1
	//var phi = 1
	args := os.Args
	for _, arg := range args[1:] {
		if arg == "meht" || arg == "mpt" || arg == "mbt" {
			siModeOptions = append(siModeOptions, arg)
			//} else if len(arg) > 4 && arg[:3] == "-phi" {
			//	if n, err := strconv.Atoi(arg[3:]); err == nil {
			//		phi = n
			//	}
		} else {
			if n, err := strconv.Atoi(arg); err == nil {
				if n >= 300000 {
					insertNum = append(insertNum, n)
				} else {
					numOfWorker = n
				}
			}
		}
	}
	sort.Ints(insertNum)
	sort.Strings(siModeOptions)
	if len(insertNum) == 0 {
		insertNum = []int{300000, 600000, 900000, 1200000, 1500000}
	}
	if len(siModeOptions) == 0 {
		siModeOptions = []string{"meht", "mpt", "mbt"}
	}
	for _, siMode := range siModeOptions {
		for _, num := range insertNum {
			filePath := "data/levelDB/config" + strconv.Itoa(num) + siMode + ".txt" //存储seHash和dbPath的文件路径
			dirs := strings.Split(filePath, "/")
			if _, err := os.Stat(strings.Join(dirs[:len(dirs)-1], "/")); os.IsNotExist(err) {
				if err := os.MkdirAll(strings.Join(dirs[:len(dirs)-1], "/"), 0750); err != nil {
					log.Fatal(err)
				}
			}
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				util.WriteStringToFile(filePath, ",data/levelDB/PrimaryDB"+strconv.Itoa(num)+siMode+
					",data/levelDB/SecondaryDB"+strconv.Itoa(num)+siMode+"\n")
			}
			mbtBucketNum := 1280
			mbtAggregation := 16
			mbtArgs := make([]interface{}, 0)
			mbtArgs = append(mbtArgs, sedb.MBTBucketNum(mbtBucketNum), sedb.MBTAggregation(mbtAggregation))
			mehtRdx := 16  //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
			mehtBc := 1280 //meht中bucket的容量，即每个bucket中最多存储的KVPair数
			mehtBs := 1    //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
			mehtArgs := make([]interface{}, 0)
			mehtArgs = append(mehtArgs, sedb.MEHTRdx(16), sedb.MEHTBc(mehtBc), sedb.MEHTBs(mehtBs))
			seHash, primaryDbPath, secondaryDbPath := sedb.ReadSEDBInfoFromFile(filePath)
			var seDB *sedb.SEDB
			//cacheEnable := false
			cacheEnable := true
			argsString := ""
			if cacheEnable {
				shortNodeCacheCapacity := 50000000
				fullNodeCacheCapacity := 50000000
				mgtNodeCacheCapacity := 100000000
				bucketCacheCapacity := 128000000
				segmentCacheCapacity := mehtBs * bucketCacheCapacity
				merkleTreeCacheCapacity := mehtBs * bucketCacheCapacity
				seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, mbtArgs, mehtArgs, cacheEnable,
					sedb.ShortNodeCacheCapacity(shortNodeCacheCapacity), sedb.FullNodeCacheCapacity(fullNodeCacheCapacity),
					sedb.MgtNodeCacheCapacity(mgtNodeCacheCapacity), sedb.BucketCacheCapacity(bucketCacheCapacity),
					sedb.SegmentCacheCapacity(segmentCacheCapacity), sedb.MerkleTreeCacheCapacity(merkleTreeCacheCapacity))
				argsString = serializeArgs(siMode, mehtRdx, mehtBc, mehtBs, cacheEnable, shortNodeCacheCapacity, fullNodeCacheCapacity, mgtNodeCacheCapacity, bucketCacheCapacity,
					segmentCacheCapacity, merkleTreeCacheCapacity, numOfWorker)
			} else {
				seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, mbtArgs, mehtArgs, cacheEnable)
				argsString = serializeArgs(siMode, mehtRdx, mehtBc, mehtBs, cacheEnable, 0, 0,
					0, 0, 0, 0,
					numOfWorker)
			}

			var duration time.Duration = 0
			var latencyDuration time.Duration = 0
			kvPairCh := make(chan util.KVPair)
			latencyDurationChList := make([]chan time.Duration, numOfWorker)
			for i := 0; i < numOfWorker; i++ {
				latencyDurationChList[i] = make(chan time.Duration)
			}
			latencyDurationList := make([]time.Duration, numOfWorker)
			doneCh := make(chan bool)
			go countLatency(&latencyDurationList, &latencyDurationChList, doneCh)
			go allocateNFTOwner("data/nft-owner", num, kvPairCh)
			batchWg := sync.WaitGroup{}
			batchWg.Add(1)
			go batchCommitter(&batchWg, seDB)
			start := time.Now()
			createWorkerPool(numOfWorker, seDB, kvPairCh, &latencyDurationChList)
			duration = time.Since(start)
			<-doneCh
			for _, du := range latencyDurationList {
				latencyDuration += du
			}
			batchWg.Wait()
			seDB.WriteSEDBInfoToFile(filePath)
			util.WriteResultToFile("data/result"+siMode, argsString+"\tInsert "+strconv.Itoa(num)+" records in "+
				duration.String()+", throughput = "+strconv.FormatFloat(float64(num)/duration.Seconds(), 'f', -1, 64)+" tps "+
				strconv.FormatFloat(duration.Seconds()/float64(num), 'f', -1, 64)+
				", average latency is "+strconv.FormatFloat(float64(latencyDuration.Milliseconds())/float64(num), 'f', -1, 64)+" mspt.\n")
			fmt.Println("Insert ", num, " records in ", duration, ", throughput = ", float64(num)/duration.Seconds(), " tps, "+
				"average latency is "+strconv.FormatFloat(float64(latencyDuration.Milliseconds())/float64(num), 'f', -1, 64)+" mspt.")
			seDB = nil
		}
	}
}
