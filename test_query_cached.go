package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jalingpp/MEST/sedb"
	"github.com/Jalingpp/MEST/util"
)

func main() {
	args := os.Args
	allocateQuery := func(path string, queryCh chan string) {
		queries := util.ReadLinesFromFile(path)
		for i, query := range queries {
			query_ := strings.Split(query, ",")
			if len(query_) < 2 {
				continue
			}
			queryCh <- query_[1]
			if i%10000 == 0 {
				fmt.Println(i)
			}
		}
		close(queryCh)
	}
	worker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, queryCh chan string, voCh chan uint, durationCh chan time.Duration, PhaseLatency *util.PhaseLatency, isBF bool) {
		for query := range queryCh {
			st := time.Now()
			_, _, proof := seDB.QueryKVPairsByHexKeyword(util.StringToHex(query), PhaseLatency, isBF)
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
	createWorkerPool := func(numOfWorker int, seDB *sedb.SEDB, queryCh chan string, voChList *[]chan uint, durationChList *[]chan time.Duration, PhaseLatency *util.PhaseLatency, isBF bool) {
		var wg sync.WaitGroup
		for i := 0; i < numOfWorker; i++ {
			wg.Add(1)
			go worker(&wg, seDB, queryCh, (*voChList)[i], (*durationChList)[i], PhaseLatency, isBF)
		}
		wg.Wait()
		for _, voCh := range *voChList {
			close(voCh)
		}
		for _, duCh := range *durationChList {
			close(duCh)
		}
	}

	serializeArgs := func(siMode string, rdx int, bc int, bs int, mbtBN int, cacheEnable bool,
		shortNodeCC int, fullNodeCC int, mgtNodeCC int, bucketCC int, segmentCC int,
		merkleTreeCC int, numOfWorker int, IsBF bool, mehtWs int, mehtSt int, mehtBFsize int, mehtBFhnum int) string {
		return "siMode: " + siMode + ",\trdx: " + strconv.Itoa(rdx) + ",\tbc: " + strconv.Itoa(bc) +
			",\tbs: " + strconv.Itoa(bs) + ",\tmbtBN: " + strconv.Itoa(mbtBN) + ",\tcacheEnable: " + strconv.FormatBool(cacheEnable) + ",\tshortNodeCacheCapacity: " +
			strconv.Itoa(shortNodeCC) + ",\tfullNodeCacheCapacity: " + strconv.Itoa(fullNodeCC) + ",\tmgtNodeCacheCapacity" +
			strconv.Itoa(mgtNodeCC) + ",\tbucketCacheCapacity: " + strconv.Itoa(bucketCC) + ",\tsegmentCacheCapacity: " +
			strconv.Itoa(segmentCC) + ",\tmerkleTreeCacheCapacity: " + strconv.Itoa(merkleTreeCC) + ",\tnumOfThread: " +
			strconv.Itoa(numOfWorker) + ",\tmehtIsBF: " + strconv.FormatBool(IsBF) + ",\tmehtWindowSize: " + strconv.Itoa(mehtWs) +
			",\tmehtStride: " + strconv.Itoa(mehtSt) + ",\tmehtBFSize: " + strconv.Itoa(mehtBFsize) + ",\tmehtBFHNum: " + strconv.Itoa(mehtBFhnum) + "."
	}
	var queryNum = make([]int, 0)
	var siModeOptions = make([]string, 0)
	var numOfWorker = 1
	for _, arg := range args[1:4] {
		if arg == "meht" || arg == "mpt" || arg == "mbt" {
			siModeOptions = append(siModeOptions, arg)
		} else {
			if n, err := strconv.Atoi(arg); err == nil {
				if n >= 10000 {
					queryNum = append(queryNum, n)
				} else {
					numOfWorker = n
				}
			}
		}
	}
	sort.Ints(queryNum)
	sort.Strings(siModeOptions)
	//if len(queryNum) == 0 {
	//	queryNum = []int{300000, 600000, 900000, 1200000, 1500000}
	//}
	//if len(siModeOptions) == 0 {
	//	siModeOptions = []string{"meht", "mpt", "mbt"}
	//}
	fmt.Println(siModeOptions)
	fmt.Println(queryNum)

	for _, siMode := range siModeOptions {
		for _, num := range queryNum {
			filePath := "data/levelDB/config" + strconv.Itoa(num) + siMode + ".txt" //存储seHash和dbPath的文件路径
			//打印args数组
			fmt.Println(args)
			mbtBucketNum, _ := strconv.Atoi(args[4]) //change
			mbtAggregation := 16
			mbtArgs := make([]interface{}, 0)
			mbtArgs = append(mbtArgs, sedb.MBTBucketNum(mbtBucketNum), sedb.MBTAggregation(mbtAggregation))
			mehtRdx := 16                      //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
			mehtBc, _ := strconv.Atoi(args[5]) //change
			mehtBs, _ := strconv.Atoi(args[6])
			mehtWs, _ := strconv.Atoi(args[8])
			mehtSt, _ := strconv.Atoi(args[9])
			mehtBFsize, _ := strconv.Atoi(args[10])
			mehtBFhnum, _ := strconv.Atoi(args[11])
			mehtIsbf := util.StringToBool(args[12])
			mehtArgs := make([]interface{}, 0)
			mehtArgs = append(mehtArgs, sedb.MEHTRdx(16), sedb.MEHTBc(mehtBc), sedb.MEHTBs(mehtBs), sedb.MEHTWs(mehtWs), sedb.MEHTSt(mehtSt), sedb.MEHTBFsize(mehtBFsize), sedb.MEHTBFHnum(mehtBFhnum), sedb.MEHTIsBF(mehtIsbf)) //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
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
				segmentCacheCapacity := bucketCacheCapacity
				merkleTreeCacheCapacity := bucketCacheCapacity
				seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, mbtArgs, mehtArgs, cacheEnable,
					sedb.ShortNodeCacheCapacity(shortNodeCacheCapacity), sedb.FullNodeCacheCapacity(fullNodeCacheCapacity),
					sedb.MgtNodeCacheCapacity(mgtNodeCacheCapacity), sedb.BucketCacheCapacity(bucketCacheCapacity),
					sedb.SegmentCacheCapacity(segmentCacheCapacity), sedb.MerkleTreeCacheCapacity(merkleTreeCacheCapacity))
				argsString = serializeArgs(siMode, mehtRdx, mehtBc, mehtBs, mbtBucketNum, cacheEnable, shortNodeCacheCapacity, fullNodeCacheCapacity, mgtNodeCacheCapacity, bucketCacheCapacity,
					segmentCacheCapacity, merkleTreeCacheCapacity, numOfWorker, mehtIsbf, mehtWs, mehtSt, mehtBFsize, mehtBFhnum)
			} else {
				seDB = sedb.NewSEDB(seHash, primaryDbPath, secondaryDbPath, siMode, mbtArgs, mehtArgs, cacheEnable)
				argsString = serializeArgs(siMode, mehtRdx, mehtBc, mehtBs, mbtBucketNum, cacheEnable, 0, 0,
					0, 0, 0, 0,
					numOfWorker, false, 0, 0, 0, 0)
			}
			var duration time.Duration = 0
			var latencyDuration time.Duration = 0
			var voSize uint = 0
			queryCh := make(chan string)
			doneCh := make(chan bool)
			voChList := make([]chan uint, numOfWorker)
			latencyDurationChList := make([]chan time.Duration, numOfWorker)

			PhaseLatency := util.NewPhaseLatency() //创建统计各阶段延迟的对象  add0128 for phase latency

			for i := 0; i < numOfWorker; i++ {
				latencyDurationChList[i] = make(chan time.Duration)
				voChList[i] = make(chan uint)
			}
			latencyDurationList := make([]time.Duration, numOfWorker)
			voList := make([]uint, numOfWorker)
			go countLatency(&latencyDurationList, &latencyDurationChList, doneCh)
			go countVo(&voList, &voChList, doneCh)
			go allocateQuery("../Synthetic/"+args[7], queryCh)
			start := time.Now()
			createWorkerPool(numOfWorker, seDB, queryCh, &voChList, &latencyDurationChList, PhaseLatency, mehtIsbf)
			duration = time.Since(start)
			<-doneCh
			<-doneCh
			for i := 0; i < numOfWorker; i++ {
				latencyDuration += latencyDurationList[i]
				voSize += voList[i]
			}
			//seDB.WriteSEDBInfoToFile(filePath)

			a := 0.9
			b := 0.1
			st := time.Now()                                    //add0128 for phase latency
			seDB.CacheAdjust(a, b)                              //add0128 for phase latency
			du := time.Since(st)                                //add0128 for phase latency
			PhaseLatency.RecordLatencyObject("cacheadjust", du) //add0128 for phase latency
			seDB.BatchCommit()                                  //add0128 for phase latency
			seDB.WriteSEDBInfoToFile(filePath)

			//统计各阶段的延迟
			PhaseLatency.CompPhaseLatency() //add0126 for phase latency

			util.WriteResultToFile("data/qresult"+siMode, argsString+"\tQuery "+strconv.Itoa(num)+" records in "+
				duration.String()+", throughput = "+strconv.FormatFloat(float64(num)/duration.Seconds(), 'f', -1, 64)+" tps, "+
				"average latency is "+strconv.FormatFloat(float64(latencyDuration.Milliseconds())/float64(num), 'f', -1, 64)+" mspt"+
				"; and vo of all proof is "+strconv.FormatUint(uint64(voSize), 10)+" B, average vo = "+
				strconv.FormatFloat(float64(voSize)/1024/float64(num), 'f', -1, 64)+" KBpt,"+
				"getkey average Latency is "+strconv.FormatFloat(float64(PhaseLatency.GetKeyLOs[0].Duration.Milliseconds())/float64(len(PhaseLatency.GetKeyLOs)), 'f', -1, 64)+" mspt,"+
				"getvalue average Latency is "+strconv.FormatFloat(float64(PhaseLatency.GetValueLOs[0].Duration.Milliseconds())/float64(len(PhaseLatency.GetValueLOs)), 'f', -1, 64)+" mspt,"+
				"getproof average Latency is "+strconv.FormatFloat(float64(PhaseLatency.GetProofLOs[0].Duration.Milliseconds())/float64(len(PhaseLatency.GetProofLOs)), 'f', -1, 64)+" mspt,"+
				"cacheadjust average Latency is "+strconv.FormatFloat(float64(PhaseLatency.CacheAdjustLOs[0].Duration.Milliseconds())/float64(len(PhaseLatency.CacheAdjustLOs)), 'f', -1, 64)+" mspt.\n")
			fmt.Println("Query ", num, " records in ", duration, ", throughput = ", float64(num)/duration.Seconds(),
				" tps, average latency is ", strconv.FormatFloat(float64(latencyDuration.Milliseconds())/float64(num), 'f', -1, 64), " mspt,"+
					"getkey average Latency is "+strconv.FormatFloat(float64(PhaseLatency.GetKeyLOs[0].Duration.Milliseconds())/float64(len(PhaseLatency.GetKeyLOs)), 'f', -1, 64)+" mspt,"+
					"getvalue average Latency is "+strconv.FormatFloat(float64(PhaseLatency.GetValueLOs[0].Duration.Milliseconds())/float64(len(PhaseLatency.GetValueLOs)), 'f', -1, 64)+" mspt,"+
					"getproof average Latency is "+strconv.FormatFloat(float64(PhaseLatency.GetProofLOs[0].Duration.Milliseconds())/float64(len(PhaseLatency.GetProofLOs)), 'f', -1, 64)+" mspt,"+
					"cacheadjust average Latency is "+strconv.FormatFloat(float64(PhaseLatency.CacheAdjustLOs[0].Duration.Milliseconds())/float64(len(PhaseLatency.CacheAdjustLOs)), 'f', -1, 64)+" mspt.")
			seDB = nil
		}
	}
}
