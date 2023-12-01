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
	//测试辅助索引查询
	allocateNFTOwner := func(filepath string, opNum int, kvPairCh chan *util.KVPair) {
		kvPairs := util.ReadNFTOwnerFromFile(filepath, opNum)
		for _, kvPair := range kvPairs {
			kvPair.SetKey(util.StringToHex(kvPair.GetKey()))
			kvPair.SetValue(util.StringToHex(kvPair.GetValue()))
			kvPairCh <- kvPair
			//fmt.Println(i)
		}
		close(kvPairCh)
	}
	worker := func(wg *sync.WaitGroup, seDB *sedb.SEDB, kvPairCh chan *util.KVPair) {
		for kvPair := range kvPairCh {
			seDB.InsertKVPair(kvPair)
		}
		wg.Done()
	}
	createWorkerPool := func(numOfWorker int, seDB *sedb.SEDB, kvPairCh chan *util.KVPair) {
		var wg sync.WaitGroup
		for i := 0; i < numOfWorker; i++ {
			wg.Add(1)
			go worker(&wg, seDB, kvPairCh)
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
	var insertNum = make([]int, 0)
	var siModeOptions = make([]string, 0)
	var numOfWorker = 2
	args := os.Args
	for _, arg := range args[1:] {
		if arg == "meht" || arg == "mpt" {
			siModeOptions = append(siModeOptions, arg)
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
		siModeOptions = []string{"meht", "mpt"}
	}
	//var siModeOptions = []string{"", "mpt"}
	//var insertNum = []int{300001}
	for _, siMode := range siModeOptions {
		for _, num := range insertNum {
			filePath := "data/levelDB/config" + strconv.Itoa(num) + siMode + ".txt" //存储seHash和dbPath的文件路径
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				util.WriteStringToFile(filePath, ",data/levelDB/PrimaryDB"+strconv.Itoa(num)+siMode+
					",data/levelDB/SecondaryDB"+strconv.Itoa(num)+siMode+"\n")
			}
			rdx := 16  //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
			bc := 1280 //meht中bucket的容量，即每个bucket中最多存储的KVPair数
			bs := 1    //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
			seHash, primaryDbPath, secondaryDbPath := sedb.ReadSEDBInfoFromFile(filePath)
			var seDB *sedb.SEDB
			//cacheEnable := false
			cacheEnable := true
			argsString := ""
			if cacheEnable {
				shortNodeCacheCapacity := 1280000
				fullNodeCacheCapacity := 1280000
				mgtNodeCacheCapacity := 1000000
				bucketCacheCapacity := 1280000
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
			kvPairCh := make(chan *util.KVPair)
			go allocateNFTOwner("data/nft-owner", num, kvPairCh)
			start = time.Now()
			createWorkerPool(numOfWorker, seDB, kvPairCh)
			duration = time.Since(start)
			seDB.WriteSEDBInfoToFile(filePath)
			//duration = time.Since(start)
			util.WriteResultToFile("data/result"+siMode, argsString+"\tInsert "+strconv.Itoa(num)+" records in "+
				duration.String()+", throughput = "+strconv.FormatFloat(float64(num)/duration.Seconds(), 'f', -1, 64)+" tps.\n")
			fmt.Println("Insert ", num, " records in ", duration, ", throughput = ", float64(num)/duration.Seconds(), " tps.")
			seDB = nil
		}
	}
}
