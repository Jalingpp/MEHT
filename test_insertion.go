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

<<<<<<< HEAD
type IntegerWithLock struct {
	number int
	lock   sync.Mutex
}

func main() {
	args := os.Args
	var batchSize, _ = strconv.Atoi(args[4]) //change
	var curStartNum = IntegerWithLock{0, sync.Mutex{}}
	var curFinishNum = IntegerWithLock{0, sync.Mutex{}}
	//var a = 0.9
	//var b = 0.1
	batchCommitterForMix := func(seDB *sedb.SEDB, flagChan chan bool) {
		//for {
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

		flagChan <- true
		//}

		//seDB.CacheAdjust(a, b)
	}
	writeWorker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, insertKVPairCh chan string, durationCh chan time.Duration) {
		for line := range insertKVPairCh {
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
			//fmt.Println("insert " + line)
			//解析line是insert还是update
			line_ := strings.Split(line, ",")
			kvPair := *util.NewKVPair(util.StringToHex(line_[1]), util.StringToHex(line_[2]))
			st := time.Now()
			if line_[0] == "insertion" || line_[0] == "insert" {
				seDB.InsertKVPair(kvPair, false)
			} else if line_[0] == "update" {
				seDB.InsertKVPair(kvPair, true)
			}
			du := time.Since(st)
			durationCh <- du //add
			curFinishNum.lock.Lock()
			curFinishNum.number++
			curFinishNum.lock.Unlock()
		}
		wg.Done()
	}

	countLatency := func(rets *[]time.Duration, durationChList *[]chan time.Duration, done chan bool) {
		wG := sync.WaitGroup{}
		wG.Add(len(*rets))
		for i := 0; i < len(*rets); i++ {
			idx := i

=======


func main() {
	args := os.Args
	var batchSize, _ = strconv.Atoi(args[4]) //change
	var curStartNum = IntegerWithLock{0, sync.Mutex{}}
	var curFinishNum = IntegerWithLock{0, sync.Mutex{}}
	//var a = 0.9
	//var b = 0.1
	batchCommitterForMix := func(seDB *sedb.SEDB, flagChan chan bool) {
		//for {
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

		flagChan <- true
		//}

		//seDB.CacheAdjust(a, b)
	}
	writeWorker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, insertKVPairCh chan string, durationCh chan time.Duration) {
		for line := range insertKVPairCh {
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
			//fmt.Println("insert " + line)
			//解析line是insert还是update
			line_ := strings.Split(line, ",")
			kvPair := *util.NewKVPair(util.StringToHex(line_[1]), util.StringToHex(line_[2]))
			st := time.Now()
			if line_[0] == "insertion" || line_[0] == "insert" {
				seDB.InsertKVPair(kvPair, false)
			} else if line_[0] == "update" {
				seDB.InsertKVPair(kvPair, true)
			}
			du := time.Since(st)
			durationCh <- du //add
			curFinishNum.lock.Lock()
			curFinishNum.number++
			curFinishNum.lock.Unlock()
		}
		wg.Done()
	}

	countLatency := func(rets *[]time.Duration, durationChList *[]chan time.Duration, done chan bool) {
		wG := sync.WaitGroup{}
		wG.Add(len(*rets))
		for i := 0; i < len(*rets); i++ {
			idx := i

>>>>>>> 5137e27dd90817a9eb72c47070fbee0267affc34
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
	createWriteWorkerPool := func(numOfWorker int, seDB *sedb.SEDB, insertKVPairCh chan string, durationChList *[]chan time.Duration, flagChan chan bool) {
		var wg sync.WaitGroup
		for i := 0; i < numOfWorker; i++ {
			wg.Add(1)
			go writeWorker(&wg, seDB, insertKVPairCh, (*durationChList)[i])
		}
		wg.Wait()

		batchCommitterForMix(seDB, flagChan)
		//stopBatchCommitterFlag = false
		//for _, duCh := range *durationChList {
		//	close(duCh)
		//}
	}
	serializeArgs := func(siMode string, rdx int, bc int, bs int, cacheEnable bool,
		shortNodeCC int, fullNodeCC int, mgtNodeCC int, bucketCC int, segmentCC int,
		merkleTreeCC int, numOfWorker int) string {
		return "siMode: " + siMode + ",\trdx: " + strconv.Itoa(rdx) + ",\tbc: " + strconv.Itoa(bc) +
			",\tbs: " + strconv.Itoa(bs) + ",\tcacheEnable: " + strconv.FormatBool(cacheEnable) + ",\tshortNodeCacheCapacity: " +
			strconv.Itoa(shortNodeCC) + ",\tfullNodeCacheCapacity: " + strconv.Itoa(fullNodeCC) + ",\tmgtNodeCacheCapacity" +
			strconv.Itoa(mgtNodeCC) + ",\tbucketCacheCapacity: " + strconv.Itoa(bucketCC) + ",\tsegmentCacheCapacity: " +
			strconv.Itoa(segmentCC) + ",\tmerkleTreeCacheCapacity: " + strconv.Itoa(merkleTreeCC) + ",\tnumOfThread: " +
			strconv.Itoa(numOfWorker) + ",\tbatchSize: " + strconv.Itoa(batchSize) + "."
	}

	var insertNum = make([]int, 0)
	var siModeOptions = make([]string, 0)
	var numOfWorker = 1 //change
	//var phi = 1
	for _, arg := range args[1:4] {
		if arg == "meht" || arg == "mpt" || arg == "mbt" {
			siModeOptions = append(siModeOptions, arg)
			//} else if len(arg) > 4 && arg[:3] == "-phi" {
			//	if n, err := strconv.Atoi(arg[3:]); err == nil {
			//		phi = n
			//	}
		} else {
			if n, err := strconv.Atoi(arg); err == nil {
				if n >= 10000 {
					insertNum = append(insertNum, n)
				} else {
					numOfWorker = n
				}
			}
		}
	}
	sort.Ints(insertNum)
	sort.Strings(siModeOptions)

	//for _, siMode := range siModeOptions {
	//	for _, num := range insertNum {
	siMode := siModeOptions[0]
	num := insertNum[0]
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
	mbtBucketNum, _ := strconv.Atoi(args[5]) //change
	mbtAggregation := 16                     //change
	mbtArgs := make([]interface{}, 0)
	mbtArgs = append(mbtArgs, sedb.MBTBucketNum(mbtBucketNum), sedb.MBTAggregation(mbtAggregation))
	mehtRdx := 16                      //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	mehtBc, _ := strconv.Atoi(args[6]) //change
	mehtBs, _ := strconv.Atoi(args[7])
	mehtArgs := make([]interface{}, 0)
	mehtArgs = append(mehtArgs, sedb.MEHTRdx(16), sedb.MEHTBc(mehtBc), sedb.MEHTBs(mehtBs))
	seHash, primaryDbPath, secondaryDbPath := sedb.ReadSEDBInfoFromFile(filePath)
	var seDB *sedb.SEDB
	//cacheEnable := false
	cacheEnable := true
	argsString := ""
	if cacheEnable {
		shortNodeCacheCapacity := 50000
		fullNodeCacheCapacity := 50000
		mgtNodeCacheCapacity := 100000
		bucketCacheCapacity := 128000
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
	latencyDurationChList := make([]chan time.Duration, numOfWorker)
	for i := 0; i < numOfWorker; i++ {
		latencyDurationChList[i] = make(chan time.Duration)
	}
	latencyDurationList := make([]time.Duration, numOfWorker)
	doneCh := make(chan bool)
	go countLatency(&latencyDurationList, &latencyDurationChList, doneCh)
<<<<<<< HEAD
	txs := util.ReadLinesFromFile("data/" + args[8])
=======
	txs := util.ReadLinesFromFile("../Synthetic/" + args[8])
>>>>>>> 5137e27dd90817a9eb72c47070fbee0267affc34
	//../Synthesis/
	txs = txs[:num+1]
	countNum := 0

	start := time.Now()
	for i := 0; i < len(txs); i += batchSize {
		//每次建立一遍
		insertKVPairCh := make(chan string, batchSize)
		flagChan := make(chan bool) //用于通知数据提交完成的通道
		for j := 0; j < batchSize; j++ {
			if i+j >= len(txs) {
				break
			}
			tx := txs[i+j]
			countNum = i + j
			if countNum%10000 == 0 {
				fmt.Println(countNum)
			}
			tx_ := strings.Split(tx, ",")
			if tx_[0] == "insertion" || tx_[0] == "update" || tx_[0] == "insert" {
				txline := txs[i+j]       //add
				insertKVPairCh <- txline //add
			} else if tx_[0] == "query" {
			}
		}
		close(insertKVPairCh)

		//TODO:在这里创建线程池并通过close通道让线程池返回
		go createWriteWorkerPool(numOfWorker, seDB, insertKVPairCh, &latencyDurationChList, flagChan) //add

		<-flagChan //阻塞等待接收提交完成通知
		//fmt.Println("insert over")
		close(flagChan)
	}
	for _, duCh := range latencyDurationChList {
		close(duCh)
	}
	<-doneCh
	for _, du := range latencyDurationList {
		latencyDuration += du
	}
	duration = time.Since(start)

	seDB.WriteSEDBInfoToFile(filePath)
	duration2 := time.Since(start)
	fmt.Println(duration)
	fmt.Println(duration2)
	util.WriteResultToFile("data/result"+siMode, argsString+"\tInsert "+strconv.Itoa(num)+" records in "+
		duration.String()+", throughput = "+strconv.FormatFloat(float64(num)/duration.Seconds(), 'f', -1, 64)+" tps "+
		strconv.FormatFloat(duration.Seconds()/float64(num), 'f', -1, 64)+
		", average latency is "+strconv.FormatFloat(float64(latencyDuration.Milliseconds())/float64(num), 'f', -1, 64)+" mspt.\n")
	fmt.Println("Insert ", num, " records in ", duration, ", throughput = ", float64(num)/duration.Seconds(), " tps, "+
		"average latency is "+strconv.FormatFloat(float64(latencyDuration.Milliseconds())/float64(num), 'f', -1, 64)+" mspt.")
	seDB = nil
<<<<<<< HEAD
}
=======
}
>>>>>>> 5137e27dd90817a9eb72c47070fbee0267affc34
