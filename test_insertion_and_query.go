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

//type IntegerWithLock struct {
//	number int
//	lock   sync.Mutex
//}
//
//type QueryTransaction struct {
//	queryKey  string
//	stratTime time.Time
//}

func main() {

	var batchSize = 10000 //change
	//var batchTimeout float64 = 100 //change
	var curStartNum = IntegerWithLock{0, sync.Mutex{}}
	var curFinishNum = IntegerWithLock{0, sync.Mutex{}}
	var stopBatchCommitterFlag = true
	var a = 0.9
	var b = 0.1
	allocateNFTOwner := func(filepath string, opNum int, insertKVPairCh chan string, queryTxnCh chan QueryTransaction, flagChan chan bool, queryChanFlag chan bool, countInsertNumCh chan int, countQueryNumCh chan int, countInsertCh chan int, countQueryCh chan int) {
		// 读文件获得transactions
		txs := util.ReadLinesFromFile(filepath)
		// 遍历txs，每batchSize个txs为一组
		var queryList []QueryTransaction
		countNum := 0
		for i := 0; i < len(txs); i += batchSize {
			insertNum := 0
			queryNum := 0
			//allocateFlagChan <- "startInsert"
			for j := 0; j < batchSize; j++ {
				if i+j >= len(txs) {
					break
				}
				tx := txs[i+j]
				countNum = i + j

				fmt.Println(countNum)

				tx_ := strings.Split(tx, ",")
				if tx_[0] == "insertion" || tx_[0] == "update" || tx_[0] == "insert" {
					insertNum += 1
					insertKVPairCh <- txs[i+j]
				} else if tx_[0] == "query" {
					queryNum += 1
					queryList = append(queryList, QueryTransaction{tx_[1], time.Now()})
				}
			}

			countInsertNumCh <- insertNum
			<-flagChan //阻塞等待接收提交完成通知
			//将查询操作分发给查询线程
			countQueryNumCh <- queryNum
			for _, queryTxn := range queryList {
				queryTxnCh <- queryTxn
			}
			queryList = queryList[:0]
			<-queryChanFlag //阻塞等待接收查询完成通知
		}
		insertNum := 0
		queryNum := 0
		countNum++
		for countNum < len(txs) {
			tx := txs[countNum]
			tx_ := strings.Split(tx, ",")
			if tx_[0] == "insertion" || tx_[0] == "update" || tx_[0] == "insert" {
				insertKVPairCh <- txs[countNum]
				insertNum += 1
			} else if tx_[0] == "query" {
				queryList = append(queryList, QueryTransaction{tx_[1], time.Now()})
				queryNum += 1
			}
			countNum++
		}
		countInsertNumCh <- insertNum
		<-flagChan //阻塞等待接收提交完成通知
		//将查询操作分发给查询线程
		countQueryNumCh <- queryNum
		for _, queryTxn := range queryList {
			queryTxnCh <- queryTxn
		}
		<-queryChanFlag //阻塞等待接收查询完成通知

		//allocateFlagChan <- "endAllocate"

		close(insertKVPairCh)
		close(queryTxnCh)
		close(queryChanFlag)
		close(flagChan)
		close(countQueryNumCh)
		close(countInsertNumCh)
		close(countInsertCh)
		close(countQueryCh)

	}
	batchCommitter := func(wG *sync.WaitGroup, seDB *sedb.SEDB, batchSizeCh chan int) {
		for stopBatchCommitterFlag {
			<-batchSizeCh                                   //read real batchsize
			curStartNum.lock.Lock()                         //阻塞新插入或查询操作
			for curStartNum.number != curFinishNum.number { //等待所有旧插入或查询操作完成
			}
			// 批量提交，即一并更新辅助索引的脏数据
			seDB.BatchCommit()
			seDB.CacheAdjust(a, b)
			seDB.BatchCommit()
			// 重置计数器
			curFinishNum.number = 0
			curStartNum.number = 0
			curStartNum.lock.Unlock()
		}
		// 所有查询与插入操作已全部结束，将尾部数据批量提交
		seDB.BatchCommit()
		wG.Done()
	}

	writeWorker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, insertKVPairCh chan string, durationCh chan time.Duration, countInsertCh chan int) {
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
			//解析line是insert还是update
			line_ := strings.Split(line, ",")
			kvPair := *util.NewKVPair(util.StringToHex(line_[1]), util.StringToHex(line_[2]))
			st := time.Now()
			if line_[0] == "insertion" || line_[0] == "insert" {
				fmt.Println(line_)
				seDB.InsertKVPair(kvPair, false)
			} else if line_[0] == "update" {
				fmt.Println(line_)
				seDB.InsertKVPair(kvPair, true)
			}
			du := time.Since(st)
			durationCh <- du
			curFinishNum.lock.Lock()
			curFinishNum.number++
			curFinishNum.lock.Unlock()

			countInsertCh <- 1
		}
		wg.Done()
	}

	queryWorker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, queryTxnCh chan QueryTransaction, voCh chan uint, durationCh chan time.Duration, countQueryCh chan int) {
		for qTxn := range queryTxnCh {
			st := qTxn.stratTime
			_, _, proof := seDB.QueryKVPairsByHexKeyword(util.StringToHex(qTxn.queryKey))
			du := time.Since(st)
			durationCh <- du
			vo := proof.GetSizeOf()
			voCh <- vo
			countQueryCh <- 1
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
	createWriteWorkerPool := func(numOfWorker int, seDB *sedb.SEDB, insertKVPairCh chan string, durationChList *[]chan time.Duration, duration1 chan time.Duration, countInsertCh chan int) {
		start := time.Now()
		var wg sync.WaitGroup
		for i := 0; i < numOfWorker; i++ {
			wg.Add(1)
			go writeWorker(&wg, seDB, insertKVPairCh, (*durationChList)[i], countInsertCh)
		}
		wg.Wait()
		//stopBatchCommitterFlag = false
		for _, duCh := range *durationChList {
			close(duCh)
		}
		tempduration1 := time.Since(start)
		duration1 <- tempduration1
	}

	createQueryWorkerPool := func(numOfWorker int, seDB *sedb.SEDB, queryTxnCh chan QueryTransaction, voChList *[]chan uint, durationChList *[]chan time.Duration, duration2 chan time.Duration, countQueryCh chan int) {
		start := time.Now()
		var wg sync.WaitGroup
		for i := 0; i < numOfWorker; i++ {
			wg.Add(1)
			go queryWorker(&wg, seDB, queryTxnCh, (*voChList)[i], (*durationChList)[i], countQueryCh)
		}
		wg.Wait()
		for _, voCh := range *voChList {
			close(voCh)
		}
		for _, duCh := range *durationChList {
			close(duCh)
		}
		tempduration2 := time.Since(start)
		duration2 <- tempduration2
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

	countInsertHelper := func(flagchan chan bool, countInsertCh chan int, countInsertNum chan int, batchSizeCh chan int) {
		for num1 := range countInsertNum {
			num2 := 0
			for {
				value := <-countInsertCh
				num2 += value
				if num2 == num1 {
					break
				}
			}
			flagchan <- true
			batchSizeCh <- num1
		}
	}

	countQueryHelper := func(queryChanFlag chan bool, countQueryCh chan int, countQueryNum chan int) {
		for num1 := range countQueryNum {
			num2 := 0
			for {
				value := <-countQueryCh
				num2 += value
				if num2 == num1 {
					break
				}
			}
			queryChanFlag <- true
		}
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

	//=============================================================================================================
	var insertNum = make([]int, 0)
	var siModeOptions = make([]string, 0)
	var numOfWorker = 1 //change
	var mission string
	//args simode,insertnum,numofworker,mission
	args := os.Args
	siModeOptions = append(siModeOptions, args[1])
	if n, err := strconv.Atoi(args[2]); err == nil {
		insertNum = append(insertNum, n)
	}
	if n, err := strconv.Atoi(args[3]); err == nil {
		numOfWorker = n
	}

	mission = args[4]

	sort.Ints(insertNum)
	sort.Strings(siModeOptions)
	if len(insertNum) == 0 {
		insertNum = []int{300000, 600000, 900000, 1200000, 1500000} //change
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
			mbtBucketNum := 1280 //change
			mbtAggregation := 16 //change
			mbtArgs := make([]interface{}, 0)
			mbtArgs = append(mbtArgs, sedb.MBTBucketNum(mbtBucketNum), sedb.MBTAggregation(mbtAggregation))
			mehtRdx := 16  //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
			mehtBc := 1280 //meht中bucket的容量，即每个bucket中最多存储的KVPair数    //change
			mehtBs := 1    //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment    //change
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
				mgtNodeCacheCapacity := 100000000
				bucketCacheCapacity := 128000000
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
				latencyDurationChList[i] = make(chan time.Duration)
			}
			latencyDurationList := make([]time.Duration, numOfWorker) //用于保存每个Worker的延迟时间
			doneCh := make(chan bool)                                 //用于通知VO大小和延迟累加完成的通道
			voChList := make([]chan uint, numOfWorker)
			voList := make([]uint, numOfWorker)
			go countVo(&voList, &voChList, doneCh)
			go countLatency(&latencyDurationList, &latencyDurationChList, doneCh)

			flagChan := make(chan bool) //用于通知数据提交完成的通道
			//allocateFlagChan := make(chan string) //用于通知数据分发完成的通道
			queryChanFlag := make(chan bool) //用于通知数据分发者查询已完成的通道

			countInsertNumCh := make(chan int)
			countQueryNumCh := make(chan int)
			countInsertCh := make(chan int, 10000)
			countQueryCh := make(chan int, 10000)
			batchSizeCh := make(chan int) //count batchsize
			insertKVPairCh := make(chan string)
			queryTxnCh := make(chan QueryTransaction)
			go allocateNFTOwner("data/Synthesis_U4", num, insertKVPairCh, queryTxnCh, flagChan, queryChanFlag, countInsertNumCh, countQueryNumCh, countInsertCh, countQueryCh)
			go countInsertHelper(flagChan, countInsertCh, countInsertNumCh, batchSizeCh)
			go countQueryHelper(queryChanFlag, countQueryCh, countQueryNumCh)
			batchWg := sync.WaitGroup{}
			batchWg.Add(1)

			go batchCommitter(&batchWg, seDB, batchSizeCh)

			duration1Ch := make(chan time.Duration)
			duration2Ch := make(chan time.Duration)
			go createWriteWorkerPool(numOfWorker, seDB, insertKVPairCh, &latencyDurationChList, duration1Ch, countInsertCh)
			go createQueryWorkerPool(numOfWorker, seDB, queryTxnCh, &voChList, &latencyDurationChList, duration2Ch, countQueryCh)
			<-doneCh
			<-doneCh
			duration1 := <-duration1Ch
			duration2 := <-duration2Ch
			close(duration1Ch)
			close(duration2Ch)
			stopBatchCommitterFlag = false
			for _, du := range latencyDurationList {
				latencyDuration += du
			}
			batchWg.Wait()
			close(batchSizeCh)

			start1 := time.Now()
			seDB.WriteSEDBInfoToFile(filePath)
			duration3 := time.Since(start1)
			duration += duration1 + duration2 + duration3
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
