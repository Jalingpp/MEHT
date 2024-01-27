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

type QueryTransaction struct {
	queryKey  string
	stratTime time.Time
}

func main() {
	args := os.Args
	var batchSize, _ = strconv.Atoi(args[4]) //change
	//var batchTimeout float64 = 100 //change
	var curStartNum = IntegerWithLock{0, sync.Mutex{}}
	var curFinishNum = IntegerWithLock{0, sync.Mutex{}}
	//var stopBatchCommitterFlag = true
	var a = 0.9
	var b = 0.1
	var cmmitTime = 0
	batchCommitterForMix := func(seDB *sedb.SEDB, flagChan chan bool) {
		//for {
		curStartNum.lock.Lock()                         //阻塞新插入或查询操作
		for curStartNum.number != curFinishNum.number { //等待所有旧插入或查询操作完成
		}
		// 批量提交，即一并更新辅助索引的脏数据
		seDB.BatchCommit()
		cmmitTime++
		if cmmitTime == 5 {
			seDB.CacheAdjust(a, b)
			seDB.BatchCommit()
			cmmitTime = 0
		}

		// 重置计数器
		curFinishNum.number = 0
		curStartNum.number = 0
		curStartNum.lock.Unlock()

		flagChan <- true
		//}

		//seDB.CacheAdjust(a, b)
	}

	writeWorker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, insertKVPairCh chan string, durationCh chan time.Duration, completeInsertCh chan int) {
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
			durationCh <- du
			orderNum, _ := strconv.Atoi(line_[3]) //add
			completeInsertCh <- orderNum          //add
			curFinishNum.lock.Lock()
			curFinishNum.number++
			curFinishNum.lock.Unlock()
		}
		wg.Done()
	}

	queryWorker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, queryTxnCh chan QueryTransaction, voCh chan uint, durationCh chan time.Duration) {
		for qTxn := range queryTxnCh {
			//fmt.Println("query " + qTxn.queryKey)
			st := qTxn.stratTime
			_, _, proof := seDB.QueryKVPairsByHexKeyword(util.StringToHex(qTxn.queryKey))
			//fmt.Println("query result " + qTxn.queryKey)
			du := time.Since(st)
			durationCh <- du
			vo := proof.GetSizeOf()
			voCh <- vo
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
				//结束时间
				wG.Done()
			}()
		}
		wG.Wait()
		done <- true
	}
	createWriteWorkerPool := func(numOfWorker int, seDB *sedb.SEDB, insertKVPairCh chan string, durationChList *[]chan time.Duration, flagChan chan bool, completeInsertCh chan int) {
		var wg sync.WaitGroup
		for i := 0; i < numOfWorker; i++ {
			wg.Add(1)
			go writeWorker(&wg, seDB, insertKVPairCh, (*durationChList)[i], completeInsertCh)
		}
		wg.Wait()

		batchCommitterForMix(seDB, flagChan)
		//stopBatchCommitterFlag = false
		//for _, duCh := range *durationChList {
		//	close(duCh)
		//}
	}

	createQueryWorkerPool := func(numOfWorker int, seDB *sedb.SEDB, queryTxnCh chan QueryTransaction, voChList *[]chan uint, durationChList *[]chan time.Duration, queryChanFlag chan bool) {
		var wg sync.WaitGroup
		for i := 0; i < numOfWorker; i++ {
			wg.Add(1)
			go queryWorker(&wg, seDB, queryTxnCh, (*voChList)[i], (*durationChList)[i])
		}
		wg.Wait()
		//for _, voCh := range *voChList {
		//	close(voCh)
		//}
		//for _, duCh := range *durationChList {
		//	close(duCh)
		//}
		queryChanFlag <- true
	}

	countVo := func(voList *[]uint, voChList *[]chan uint, done chan bool) {
		wG := sync.WaitGroup{}
		size := len(*voList)
		wG.Add(size)
		for i := 0; i < size; i++ {
			idx := i
			go func() {
				ch := (*voChList)[idx]
				for vo := range ch {
					(*voList)[idx] += vo
				}
				wG.Done()
			}()
		}
		wG.Wait()
		done <- true
	}

	serializeArgs := func(siMode string, rdx int, bc int, bs int, cacheEnable bool,
		shortNodeCC int, fullNodeCC int, mgtNodeCC int, bucketCC int, segmentCC int,
		merkleTreeCC int, numOfWorker int, mission string) string {
		return "mission: " + mission + ",\tsiMode: " + siMode + ",\trdx: " + strconv.Itoa(rdx) + ",\tbc: " + strconv.Itoa(bc) +
			",\tbs: " + strconv.Itoa(bs) + ",\tcacheEnable: " + strconv.FormatBool(cacheEnable) + ",\tshortNodeCacheCapacity: " +
			strconv.Itoa(shortNodeCC) + ",\tfullNodeCacheCapacity: " + strconv.Itoa(fullNodeCC) + ",\tmgtNodeCacheCapacity" +
			strconv.Itoa(mgtNodeCC) + ",\tbucketCacheCapacity: " + strconv.Itoa(bucketCC) + ",\tsegmentCacheCapacity: " +
			strconv.Itoa(segmentCC) + ",\tmerkleTreeCacheCapacity: " + strconv.Itoa(merkleTreeCC) + ",\tnumOfThread: " +
			strconv.Itoa(numOfWorker) + "."
	}

	resetQueryStartTime := func(queryMap map[int]QueryTransaction, completeInsertCh chan int) {
		waterLine := 0
		for orderNum := range completeInsertCh {
			waterLine = orderNum + 1
			for {
				queryTxn, exist := queryMap[waterLine]
				if !exist {
					break
				}
				queryTxn.stratTime = time.Now()
				//将queryTxn重新插入到map中
				queryMap[waterLine] = queryTxn
				waterLine++
			}
		}
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
	//if len(insertNum) == 0 {
	//	insertNum = []int{200000} //change
	//}
	//if len(siModeOptions) == 0 {
	//	siModeOptions = []string{"mpt"}
	//}

	mission := "test0123"
	fmt.Println(siModeOptions)
	fmt.Println(insertNum)

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
	//mbtBucketNum := 1280 //change
	mbtBucketNum, _ := strconv.Atoi(args[5]) //change
	mbtAggregation := 16                     //change
	mbtArgs := make([]interface{}, 0)
	mbtArgs = append(mbtArgs, sedb.MBTBucketNum(mbtBucketNum), sedb.MBTAggregation(mbtAggregation))
	mehtRdx := 16 //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	//mehtBc := 1280 //meht中bucket的容量，即每个bucket中最多存储的KVPair数    //change
	mehtBc, _ := strconv.Atoi(args[6]) //change
	//mehtBs := 1    //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment    //change
	mehtBs, _ := strconv.Atoi(args[7])
	mehtArgs := make([]interface{}, 0)
	mehtArgs = append(mehtArgs, sedb.MEHTRdx(16), sedb.MEHTBc(mehtBc), sedb.MEHTBs(mehtBs))
	seHash, primaryDbPath, secondaryDbPath := sedb.ReadSEDBInfoFromFile(filePath)
	var seDB *sedb.SEDB
	//cacheEnable := false
	cacheEnable := true
	argsString := ""
	if cacheEnable {
		shortNodeCacheCapacity := 5000
		fullNodeCacheCapacity := 5000
		mgtNodeCacheCapacity := 100000
		bucketCacheCapacity := 128000
		segmentCacheCapacity := mehtBs * bucketCacheCapacity
		merkleTreeCacheCapacity := mehtBs * bucketCacheCapacity
		seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, mbtArgs, mehtArgs, cacheEnable,
			sedb.ShortNodeCacheCapacity(shortNodeCacheCapacity), sedb.FullNodeCacheCapacity(fullNodeCacheCapacity),
			sedb.MgtNodeCacheCapacity(mgtNodeCacheCapacity), sedb.BucketCacheCapacity(bucketCacheCapacity),
			sedb.SegmentCacheCapacity(segmentCacheCapacity), sedb.MerkleTreeCacheCapacity(merkleTreeCacheCapacity))
		argsString = serializeArgs(siMode, mehtRdx, mehtBc, mehtBs, cacheEnable, shortNodeCacheCapacity, fullNodeCacheCapacity, mgtNodeCacheCapacity, bucketCacheCapacity,
			segmentCacheCapacity, merkleTreeCacheCapacity, numOfWorker, mission)
	} else {
		seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, mbtArgs, mehtArgs, cacheEnable)
		argsString = serializeArgs(siMode, mehtRdx, mehtBc, mehtBs, cacheEnable, 0, 0,
			0, 0, 0, 0,
			numOfWorker, mission)
	}

	var duration time.Duration = 0
	var latencyDuration time.Duration = 0

	latencyDurationChList := make([]chan time.Duration, numOfWorker)
	for i := 0; i < numOfWorker; i++ {
		latencyDurationChList[i] = make(chan time.Duration, 10000)
	}
	latencyDurationList := make([]time.Duration, numOfWorker) //用于保存每个Worker的延迟时间
	doneCh := make(chan bool)                                 //用于通知VO大小和延迟累加完成的通道
	voChList := make([]chan uint, numOfWorker)
	voList := make([]uint, numOfWorker)

	completedInsertCh := make(chan int) //记录已经完成的插入操作序号的通道  //add

	for i := 0; i < numOfWorker; i++ {
		latencyDurationChList[i] = make(chan time.Duration)
		voChList[i] = make(chan uint)
		voList[i] = 0
	}

	go countVo(&voList, &voChList, doneCh)
	go countLatency(&latencyDurationList, &latencyDurationChList, doneCh)

	//allocate code
	txs := util.ReadLinesFromFile("../Synthetic/" + args[8])
	txs = txs[:num+1]
	countNum := 0

	queryMap := make(map[int]QueryTransaction)          //add
	go resetQueryStartTime(queryMap, completedInsertCh) //add

	start := time.Now()
	for i := 0; i < len(txs); i += batchSize {
		//每次建立一遍
		insertKVPairCh := make(chan string, batchSize)
		queryTxnCh := make(chan QueryTransaction, batchSize)
		flagChan := make(chan bool)      //用于通知数据提交完成的通道
		queryChanFlag := make(chan bool) //用于通知数据分发者查询已完成的通道
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
				txline := txs[i+j] + "," + strconv.Itoa(countNum) //add
				insertKVPairCh <- txline                          //add
			} else if tx_[0] == "query" {
				queryMap[countNum] = QueryTransaction{tx_[1], time.Now()} //add
			}
		}
		close(insertKVPairCh)

		//TODO:在这里创建线程池并通过close通道让线程池返回
		go createWriteWorkerPool(numOfWorker, seDB, insertKVPairCh, &latencyDurationChList, flagChan, completedInsertCh) //add

		<-flagChan //阻塞等待接收提交完成通知
		//fmt.Println("insert over")
		close(flagChan)

		//fmt.Println(len(queryMap))          //add
		for _, queryTxn := range queryMap { //add
			queryTxnCh <- queryTxn
		}
		close(queryTxnCh)
		queryMap = make(map[int]QueryTransaction) //add
		//TODO:在这里创建线程池并通过close通道让线程池返回
		go createQueryWorkerPool(numOfWorker, seDB, queryTxnCh, &voChList, &latencyDurationChList, queryChanFlag)

		<-queryChanFlag //阻塞等待接收查询完成通知
		//fmt.Println("query over")
		close(queryChanFlag)

	}

	for _, duCh := range latencyDurationChList {
		close(duCh)
	}

	for _, voCh := range voChList {
		close(voCh)
	}

	<-doneCh
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

}
