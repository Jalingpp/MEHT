package main

import (
	"MEHT/sedb"
	"MEHT/util"
	"fmt"
	"strconv"
	"sync"
	"time"
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
	var queryNum = []int{300000, 600000, 900000, 1200000, 1500000}
	var siModeOptions = []string{"", "mpt"}
	for _, siModeOption := range siModeOptions {
		for _, num := range queryNum {
			filePath := "data/levelDB/config" + strconv.Itoa(num) + siModeOption + ".txt" //存储seHash和dbPath的文件路径
			siMode := ""
			if len(siModeOption) == 0 {
				siMode = "meht"
			} else {
				siMode = "mpt"
			}
			rdx := 16  //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
			bc := 1280 //meht中bucket的容量，即每个bucket中最多存储的KVPair数
			bs := 1    //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
			numOfWorker := 2
			seHash, primaryDbPath, secondaryDbPath := sedb.ReadSEDBInfoFromFile(filePath)
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
			var start time.Time
			var duration time.Duration = 0
			queryCh := make(chan string)
			done := make(chan bool)
			go allocateQueryOwner("data/nft-owner", num, queryCh)
			start = time.Now()
			createWorkerPool(numOfWorker, seDB, queryCh)
			<-done
			seDB.WriteSEDBInfoToFile(filePath)
			duration = time.Since(start)
			util.WriteResultToFile("data/qresult"+siModeOption+"WithoutVO", argsString+"\tQuery "+strconv.Itoa(num)+" records in "+
				duration.String()+", throughput = "+strconv.FormatFloat(float64(num)/duration.Seconds(), 'f', -1, 64)+" tps, "+
				"average latency is "+strconv.FormatFloat(float64(duration.Milliseconds())/float64(num), 'f', -1, 64)+" mspt.\n")
			fmt.Println("Query ", num, " records in ", duration, ", throughput = ", float64(num)/duration.Seconds(), " tps.")
			seDB = nil
		}
	}
}
