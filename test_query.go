package main

import (
	"MEHT/sedb"
	"MEHT/util"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

func main() {
	//测试SEDB查询
	allocateQuery := func(dirPath string, opNum int, queryCh chan string, phi int) {
		// PHI 代表分割分位数
		queries := util.ReadQueryFromFile(dirPath, opNum)
		wG := sync.WaitGroup{}
		wG.Add(phi)
		batchNum := len(queries)/phi + 1
		for i := 0; i < phi; i++ {
			idx := i
			go func() {
				st := idx * batchNum
				var ed int
				if idx != phi-1 {
					ed = (idx + 1) * batchNum
				} else {
					ed = len(queries)
				}
				for _, query := range queries[st:ed] {
					queryCh <- query
				}
				wG.Done()
			}()
		}
		wG.Wait()
		close(queryCh)
	}
	worker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, queryCh chan string, voCh chan uint, durationCh chan time.Duration) {
		for query := range queryCh {
			st := time.Now()
			_, _, proof := seDB.QueryKVPairsByHexKeyword(util.StringToHex(query))
			du := time.Since(st)
			durationCh <- du
			vo := proof.GetSizeOf()
			voCh <- vo
		}
		wg.Done()
	}
	countLatency := func(durationList *[]time.Duration, durationChList *[]chan time.Duration, done chan bool) {
		wG := sync.WaitGroup{}
		size := len(*durationList)
		wG.Add(size)
		for i := 0; i < size; i++ {
			idx := i
			go func() {
				ch := (*durationChList)[idx]
				for du := range ch {
					(*durationList)[idx] += du
				}
				wG.Done()
			}()
		}
		wG.Wait()
		done <- true
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
	createWorkerPool := func(numOfWorker int, seDB *sedb.SEDB, queryCh chan string, voChList *[]chan uint, durationChList *[]chan time.Duration) {
		var wg sync.WaitGroup
		for i := 0; i < numOfWorker; i++ {
			wg.Add(1)
			go worker(&wg, seDB, queryCh, (*voChList)[i], (*durationChList)[i])
		}
		wg.Wait()
		for _, voCh := range *voChList {
			close(voCh)
		}
		for _, duCh := range *durationChList {
			close(duCh)
		}
	}

	serializeArgs := func(siMode string, rdx int, bc int, bs int, cacheEnable bool,
		shortNodeCC int, fullNodeCC int, mgtNodeCC int, bucketCC int, segmentCC int,
		merkleTreeCC int, numOfWorker int, phi int) string {
		return "siMode: " + siMode + ",\trdx: " + strconv.Itoa(rdx) + ",\tbc: " + strconv.Itoa(bc) +
			",\tbs: " + strconv.Itoa(bs) + ",\tcacheEnable: " + strconv.FormatBool(cacheEnable) + ",\tshortNodeCacheCapacity: " +
			strconv.Itoa(shortNodeCC) + ",\tfullNodeCacheCapacity: " + strconv.Itoa(fullNodeCC) + ",\tmgtNodeCacheCapacity" +
			strconv.Itoa(mgtNodeCC) + ",\tbucketCacheCapacity: " + strconv.Itoa(bucketCC) + ",\tsegmentCacheCapacity: " +
			strconv.Itoa(segmentCC) + ",\tmerkleTreeCacheCapacity: " + strconv.Itoa(merkleTreeCC) + ",\tnumOfThread: " +
			strconv.Itoa(numOfWorker) + ",\tphi: " + strconv.Itoa(phi) + "."
	}
	var queryNum = make([]int, 0)
	var siModeOptions = make([]string, 0)
	var numOfWorker = 2
	var phi = 1
	args := os.Args
	for _, arg := range args[1:] {
		if arg == "meht" || arg == "mpt" {
			siModeOptions = append(siModeOptions, arg)
		} else if len(arg) > 4 && arg[:3] == "-phi" {
			if n, err := strconv.Atoi(arg[3:]); err == nil {
				phi = n
			}
		} else {
			if n, err := strconv.Atoi(arg); err == nil {
				if n >= 300000 {
					queryNum = append(queryNum, n)
				} else {
					numOfWorker = n
				}
			}
		}
	}
	sort.Ints(queryNum)
	sort.Strings(siModeOptions)
	if len(queryNum) == 0 {
		queryNum = []int{300000, 600000, 900000, 1200000, 1500000}
	}
	if len(siModeOptions) == 0 {
		siModeOptions = []string{"meht", "mpt"}
	}
	for _, siMode := range siModeOptions {
		for _, num := range queryNum {
			filePath := "data/levelDB/config" + strconv.Itoa(num) + siMode + ".txt" //存储seHash和dbPath的文件路径
			rdx := 16                                                               //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
			bc := 1280                                                              //meht中bucket的容量，即每个bucket中最多存储的KVPair数
			bs := 1                                                                 //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
			seHash, primaryDbPath, secondaryDbPath := sedb.ReadSEDBInfoFromFile(filePath)
			var seDB *sedb.SEDB
			//cacheEnable := false
			cacheEnable := true
			argsString := ""
			if cacheEnable {
				shortNodeCacheCapacity := rdx * num / 12 * 7
				fullNodeCacheCapacity := rdx * num / 12 * 7
				mgtNodeCacheCapacity := 100000000
				bucketCacheCapacity := 128000000
				segmentCacheCapacity := bs * bucketCacheCapacity
				merkleTreeCacheCapacity := bs * bucketCacheCapacity
				seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, "test", rdx, bc, bs, cacheEnable,
					sedb.ShortNodeCacheCapacity(shortNodeCacheCapacity), sedb.FullNodeCacheCapacity(fullNodeCacheCapacity),
					sedb.MgtNodeCacheCapacity(mgtNodeCacheCapacity), sedb.BucketCacheCapacity(bucketCacheCapacity),
					sedb.SegmentCacheCapacity(segmentCacheCapacity), sedb.MerkleTreeCacheCapacity(merkleTreeCacheCapacity))
				argsString = serializeArgs(siMode, rdx, bc, bs, cacheEnable, shortNodeCacheCapacity, fullNodeCacheCapacity, mgtNodeCacheCapacity, bucketCacheCapacity,
					segmentCacheCapacity, merkleTreeCacheCapacity, numOfWorker, phi)
			} else {
				seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, "test", rdx, bc, bs, cacheEnable)
				argsString = serializeArgs(siMode, rdx, bc, bs, cacheEnable, 0, 0,
					0, 0, 0, 0,
					numOfWorker, phi)
			}
			var duration time.Duration = 0
			var latencyDuration time.Duration = 0
			var voSize uint = 0
			queryCh := make(chan string)
			doneCh := make(chan bool)
			voChList := make([]chan uint, numOfWorker)
			latencyDurationChList := make([]chan time.Duration, numOfWorker)
			for i := 0; i < numOfWorker; i++ {
				latencyDurationChList[i] = make(chan time.Duration)
				voChList[i] = make(chan uint)
			}
			latencyDurationList := make([]time.Duration, numOfWorker)
			voList := make([]uint, numOfWorker)
			go countLatency(&latencyDurationList, &latencyDurationChList, doneCh)
			go countVo(&voList, &voChList, doneCh)
			go allocateQuery("data/", num, queryCh, phi)
			start := time.Now()
			createWorkerPool(numOfWorker, seDB, queryCh, &voChList, &latencyDurationChList)
			duration = time.Since(start)
			<-doneCh
			<-doneCh
			for i := 0; i < numOfWorker; i++ {
				latencyDuration += latencyDurationList[i]
				voSize += voList[i]
			}
			//seDB.WriteSEDBInfoToFile(filePath)
			util.WriteResultToFile("data/qresult"+siMode, argsString+"\tQuery "+strconv.Itoa(num)+" records in "+
				duration.String()+", throughput = "+strconv.FormatFloat(float64(num)/duration.Seconds(), 'f', -1, 64)+" tps, "+
				"average latency is "+strconv.FormatFloat(float64(latencyDuration.Milliseconds())/float64(num), 'f', -1, 64)+" mspt"+
				"; and vo of all proof is "+strconv.FormatUint(uint64(voSize), 10)+"B, average vo = "+
				strconv.FormatFloat(float64(voSize)/1024/float64(num), 'f', -1, 64)+" KBpt.\n")
			fmt.Println("Query ", num, " records in ", duration, ", throughput = ", float64(num)/duration.Seconds(),
				" tps, average latency is ", strconv.FormatFloat(float64(latencyDuration.Milliseconds())/float64(num), 'f', -1, 64), " mspt.")
			seDB = nil
		}
	}
}
