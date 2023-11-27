package sedb

import (
	"MEHT/meht"
	"MEHT/mpt"
	"MEHT/util"
	"encoding/hex"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"os"
	"strings"
	"sync"
)

//func NewSEDB(seh []byte, dbPath string, siMode string, rdx int, bc int, bs int) *SEDB {}：新建一个SEDB
//func (sedb *SEDB) GetStorageEngine() *StorageEngine {}： 获取SEDB中的StorageEngine，如果为空，从db中读取se
//func (sedb *SEDB) InsertKVPair(kvpair *util.KVPair) *SEDBProof {}： 向SEDB中插入一条记录,返回插入证明
//func (sedb *SEDB) QueryKVPairsByHexKeyword(Hexkeyword string) (string, []*util.KVPair, *SEDBProof) {}: 根据十六进制的非主键Hexkeyword查询完整的kvpair
//func (sedb *SEDB) PrintKVPairsQueryResult(qkey string, qvalue string, qresult []*util.KVPair, qproof *SEDBProof) {}: 打印非主键查询结果
//func (sedb *SEDB) VerifyQueryResult(pk string, result []*util.KVPair, sedbProof *SEDBProof) bool {}: 验证查询结果
//func (sedb *SEDB) WriteSEDBInfoToFile(filePath string) {}： 写seHash和dbPath到文件
//func ReadSEDBInfoFromFile(filePath string) ([]byte, string) {}： 从文件中读取seHash和dbPath
//func (sedb *SEDB) PrintSEDB() {}： 打印SEDB

// MPT Cache
type FullNodeCacheCapacity int
type ShortNodeCacheCapacity int

var DefaultFullNodeCacheCapacity = FullNodeCacheCapacity(128)
var DefaultShortNodeCacheCapacity = ShortNodeCacheCapacity(128)

// MEHT Cache
type MgtNodeCacheCapacity int
type BucketCacheCapacity int
type SegmentCacheCapacity int
type MerkleTreeCacheCapacity int

var DefaultMgtNodeCacheCapacity = MgtNodeCacheCapacity(2 * 128)
var DefaultBucketCacheCapacity = BucketCacheCapacity(128)
var DefaultSegmentCacheCapacity = SegmentCacheCapacity(2 * 128)
var DefaultMerkleTreeCapacity = MerkleTreeCacheCapacity(2 * 128)

type SEDB struct {
	se              *StorageEngine //搜索引擎的指针
	seHash          []byte         //搜索引擎序列化后的哈希值
	primaryDb       *leveldb.DB    //底层存储的指针
	secondaryDb     *leveldb.DB
	primaryDbPath   string //底层存储的文件路径
	secondaryDbPath string

	siMode        string //se的参数，辅助索引类型，meht或mpt
	mehtName      string //se的参数，meht的名字
	rdx           int    //se的参数，meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	bc            int    //se的参数，meht中bucket的容量，即每个bucket中最多存储的KVPair数
	bs            int    //se的参数，meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
	cacheEnable   bool
	cacheCapacity []interface{}
	latch         sync.RWMutex
	updateLatch   sync.Mutex
}

// NewSEDB() *SEDB: 返回一个新的SEDB
func NewSEDB(seh []byte, primaryDbPath string, secondaryDbPath string, siMode string, mehtName string, rdx int, bc int, bs int, cacheEnabled bool, cacheCapacity ...interface{}) *SEDB {
	//打开或创建数据库
	primaryDb, err := leveldb.OpenFile(primaryDbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	secondaryDb, err := leveldb.OpenFile(secondaryDbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	//mgtNodeCacheCapacity := DefaultMgtNodeCacheCapacity
	//bucketNodeCacheCapacity := DefaultBucketCacheCapacity
	//segmentCacheCapacity := DefaultSegmentCacheCapacity
	//merkleTreeCacheCapacity := DefaultMerkleTreeCapacity
	//for _, capacity := range cacheCapacity {
	//	switch capacity.(type) {
	//	case MgtNodeCacheCapacity:
	//		mgtNodeCacheCapacity = capacity.(MgtNodeCacheCapacity)
	//	case BucketCacheCapacity:
	//		bucketNodeCacheCapacity = capacity.(BucketCacheCapacity)
	//	case SegmentCacheCapacity:
	//		segmentCacheCapacity = capacity.(SegmentCacheCapacity)
	//	case MerkleTreeCacheCapacity:
	//		merkleTreeCacheCapacity = capacity.(MerkleTreeCacheCapacity)
	//	}
	//}
	return &SEDB{nil, seh, primaryDb, secondaryDb, primaryDbPath, secondaryDbPath,
		siMode, mehtName, rdx, bc, bs,
		cacheEnabled, cacheCapacity, sync.RWMutex{}, sync.Mutex{}}
}

// 获取SEDB中的StorageEngine，如果为空，从db中读取se
func (sedb *SEDB) GetStorageEngine() *StorageEngine {
	//如果se为空，从db中读取se
	if sedb.se == nil && sedb.seHash != nil && len(sedb.seHash) != 0 && sedb.latch.TryLock() { // 只允许一个线程重构se
		if seString, error_ := sedb.primaryDb.Get(sedb.seHash, nil); error_ == nil {
			se, _ := DeserializeStorageEngine(seString, sedb.cacheEnable, sedb.cacheCapacity)
			sedb.se = se
		}
		sedb.latch.Unlock()
	}
	for sedb.se == nil && sedb.seHash != nil && len(sedb.seHash) != 0 {
	} // 其余线程等待se重构
	return sedb.se
}

// 向SEDB中插入一条记录,返回插入证明
func (sedb *SEDB) InsertKVPair(kvpair *util.KVPair) *SEDBProof {
	//如果是第一次插入
	if sedb.GetStorageEngine() == nil && sedb.latch.TryLock() { // 只允许一个线程新建se
		//创建一个新的StorageEngine
		sedb.se = NewStorageEngine(sedb.siMode, sedb.mehtName, sedb.rdx, sedb.bc, sedb.bs, sedb.cacheEnable,
			sedb.cacheCapacity)
		sedb.latch.Unlock()
	}
	for sedb.se == nil {
	} // 其余线程等待se创建
	//向StorageEngine中插入一条记录
	//primaryProof, secondaryMPTProof, secondaryMEHTProof := sedb.GetStorageEngine().Insert(kvpair, sedb.db)
	sedb.GetStorageEngine().Insert(kvpair, sedb.primaryDb, sedb.secondaryDb)
	//更新seHash，并将se更新至db
	sedb.se.UpdateStorageEngineToDB(sedb.primaryDb) // 保证sedb留存的seHash与se实际的Hash一致
	sedb.updateLatch.Lock()
	sedb.se.updateLatch.Lock()
	sedb.seHash = sedb.se.seHash
	sedb.se.updateLatch.Unlock()
	sedb.updateLatch.Unlock()
	//var pProof []*mpt.MPTProof
	//pProof = append(pProof, primaryProof)
	//sedbProof := NewSEDBProof(pProof, secondaryMPTProof, secondaryMEHTProof)
	//返回插入结果
	//return sedbProof
	return nil
}

func generatePrimaryKey(primaryKeys []string, primaryKeyCh chan string) {
	for _, key := range primaryKeys {
		primaryKeyCh <- key
	}
	close(primaryKeyCh)
}

func createWorkerPool(numOfWorker int, sedb *SEDB, primaryKeyCh chan string, lock *sync.Mutex, queryResult *[]*util.KVPair, primaryProof *[]*mpt.MPTProof) {
	wg := sync.WaitGroup{}
	wg.Add(numOfWorker)
	for i := 0; i < numOfWorker; i++ {
		go workerForPrimarySearch(&wg, sedb, primaryKeyCh, lock, queryResult, primaryProof)
	}
	wg.Wait()
}

func workerForPrimarySearch(wg *sync.WaitGroup, sedb *SEDB, primaryKeyCh chan string, lock *sync.Mutex, queryResult *[]*util.KVPair, primaryProof *[]*mpt.MPTProof) {
	for primaryKey := range primaryKeyCh {
		qV, pProof := sedb.GetStorageEngine().GetPrimaryIndex(sedb.primaryDb).QueryByKey(primaryKey, sedb.primaryDb, false)
		//用qV和primarykeys[i]构造一个kvpair
		kvpair := util.NewKVPair(primaryKey, qV)
		lock.Lock()
		//把kvpair加入queryResult
		*queryResult = append(*queryResult, kvpair)
		//把pProof加入primaryProof
		*primaryProof = append(*primaryProof, pProof)
		lock.Unlock()
	}
	wg.Done()
}

var sum = 0

// QueryKVPairsByHexKeyword 根据十六进制的非主键Hexkeyword查询完整的kvpair
func (sedb *SEDB) QueryKVPairsByHexKeyword(Hexkeyword string) (string, []*util.KVPair, *SEDBProof) {
	if sedb.GetStorageEngine() == nil {
		//fmt.Println("SEDB is empty!")
		return "", nil, nil
	}
	//根据Hexkeyword在非主键索引中查询
	var primaryKey string
	var secondaryMPTProof *mpt.MPTProof
	var secondaryMEHTProof *meht.MEHTProof
	var primaryProof []*mpt.MPTProof
	var queryResult []*util.KVPair
	var lock sync.Mutex
	primaryKeyCh := make(chan string)
	if sedb.siMode == "mpt" {
		//now := time.Now()
		primaryKey, secondaryMPTProof = sedb.GetStorageEngine().GetSecondaryIndex_mpt(sedb.secondaryDb).QueryByKey(Hexkeyword, sedb.secondaryDb, false)
		//fmt.Println("secondMptProof cost ", time.Since(now).Milliseconds()*100, " .")
		secondaryMEHTProof = nil
		//根据primaryKey在主键索引中查询
		if primaryKey == "" {
			//sum++
			//fmt.Println("No such key!", "     ", sum)
			return "", nil, NewSEDBProof(nil, secondaryMPTProof, secondaryMEHTProof)
		}
		primaryKeys := strings.Split(primaryKey, ",")
		//now = time.Now()
		go generatePrimaryKey(primaryKeys, primaryKeyCh)
		createWorkerPool(len(primaryKeys)/2+1, sedb, primaryKeyCh, &lock, &queryResult, &primaryProof)
		//fmt.Println("primaryMptProof cost ", time.Since(now).Milliseconds()*100, " .")
		return primaryKey, queryResult, NewSEDBProof(primaryProof, secondaryMPTProof, secondaryMEHTProof)
	} else if sedb.siMode == "meht" {
		secondaryMPTProof = nil
		//now := time.Now()
		pKey, qbucket, segkey, isSegExist, index := sedb.GetStorageEngine().GetSecondaryIndex_meht(sedb.secondaryDb).QueryValueByKey(Hexkeyword, sedb.secondaryDb)
		//fmt.Println("getQkeyProof cost ", time.Since(now).Milliseconds()*100, " .")
		primaryKey = pKey
		//根据primaryKey在主键索引中查询，同时构建MEHT的查询证明
		ch := make(chan *meht.MEHTProof)
		go func(ch chan *meht.MEHTProof) {
			//now := time.Now()
			seMEHTProof := sedb.GetStorageEngine().GetSecondaryIndex_meht(sedb.secondaryDb).GetQueryProof(qbucket, segkey, isSegExist, index, sedb.secondaryDb)
			//fmt.Println("mehtProof cost ", time.Since(now).Milliseconds(), " .")
			ch <- seMEHTProof
		}(ch)
		//根据primaryKey在主键索引中查询
		if primaryKey == "" {
			sum++
			fmt.Println("No such key!", "     ", sum)
		} else {
			primaryKeys := strings.Split(primaryKey, ",")
			//now := time.Now()
			go generatePrimaryKey(primaryKeys, primaryKeyCh)
			createWorkerPool(len(primaryKeys)/2+1, sedb, primaryKeyCh, &lock, &queryResult, &primaryProof)
			//fmt.Println("primaryProof cost ", time.Since(now).Milliseconds()*100, " .")
		}
		secondaryMEHTProof = <-ch
		return primaryKey, queryResult, NewSEDBProof(primaryProof, secondaryMPTProof, secondaryMEHTProof)
	} else {
		fmt.Println("siMode is wrong!")
		return "", nil, nil
	}
}

// 打印非主键查询结果
func (sedb *SEDB) PrintKVPairsQueryResult(qkey string, qvalue string, qresult []*util.KVPair, qproof *SEDBProof) {
	fmt.Printf("打印查询结果-------------------------------------------------------------------------------------------\n")
	fmt.Printf("查询关键字为%s的主键为:%s\n", qkey, qvalue)
	for i := 0; i < len(qresult); i++ {
		fmt.Printf("查询结果[%d]:key=%s,value=%s\n", i, util.HexToString(qresult[i].GetKey()), util.HexToString(qresult[i].GetValue()))
	}
	//fmt.Printf("查询证明为:\n")
	if qproof != nil {
		//qproof.PrintSEDBProof()
	} else {
		fmt.Printf("查询证明为空\n")
	}
}

// 验证查询结果
func (sedb *SEDB) VerifyQueryResult(pk string, result []*util.KVPair, sedbProof *SEDBProof) bool {
	r := false
	rCh := make(chan bool)
	fmt.Printf("验证查询结果-------------------------------------------------------------------------------------------\n")
	//验证非主键查询结果
	fmt.Printf("验证非主键查询结果:")
	if sedb.siMode == "mpt" {
		r = sedb.se.GetSecondaryIndex_mpt(sedb.secondaryDb).VerifyQueryResult(pk, sedbProof.GetSecondaryMPTIndexProof())
	} else if sedb.siMode == "meht" {
		r = meht.VerifyQueryResult(pk, sedbProof.GetSecondaryMEHTIndexProof())
	} else {
		fmt.Println("siMode is wrong!")
		return false
	}
	if !r {
		fmt.Println("非主键查询结果验证失败！")
		return false
	}
	//验证主键查询结果
	if pk == "" {
		fmt.Println("非主键查询结果为空！")
		return false
	}
	fmt.Printf("验证主键查询结果:\n")
	primaryIndexProof := sedbProof.GetPrimaryIndexProof()
	for i := 0; i < len(result); i++ {
		go func(result string, proof *mpt.MPTProof) {
			rCh <- sedb.se.GetPrimaryIndex(sedb.primaryDb).VerifyQueryResult(result, proof)
		}(result[i].GetValue(), primaryIndexProof[i])
	}
	for i := 0; i < len(result); i++ {
		r = <-rCh
		if !r {
			fmt.Println("主键查询结果验证失败!")
			return r
		}
	}
	return r
}

// 写seHash和dbPath到文件
func (sedb *SEDB) WriteSEDBInfoToFile(filePath string) {
	se := sedb.GetStorageEngine()
	if err := sedb.primaryDb.Put(se.seHash, SerializeStorageEngine(se), nil); err != nil {
		fmt.Println("Insert StorageEngine to DB error:", err)
	}
	if sedb.cacheEnable {
		sedb.GetStorageEngine().GetPrimaryIndex(sedb.primaryDb).PurgeCache()
		switch sedb.siMode {
		case "meht":
			sedb.GetStorageEngine().GetSecondaryIndex_meht(sedb.secondaryDb).PurgeCache()
		case "mpt":
			sedb.GetStorageEngine().GetSecondaryIndex_mpt(sedb.secondaryDb).PurgeCache()
		default:
			panic("Unknown siMode when purge cache.")
		}
	}
	data := hex.EncodeToString(sedb.seHash) + "," + sedb.primaryDbPath + "," + sedb.secondaryDbPath + "\n"
	util.WriteStringToFile(filePath, data)
}

// 从文件中读取seHash和dbPath
func ReadSEDBInfoFromFile(filePath string) ([]byte, string, string) {
	data, _ := util.ReadStringFromFile(filePath)
	seh_dbPath := strings.Split(data, ",")
	if len(seh_dbPath) != 3 {
		fmt.Println("seHash and dbPath don't exist!")
		return nil, "", ""
	}
	seh, _ := hex.DecodeString(seh_dbPath[0])
	primaryDbPath := util.Strip(seh_dbPath[1], "\n\t\r")
	secondaryDbPath := util.Strip(seh_dbPath[2], "\n\t\r")
	if _, err := os.Stat(primaryDbPath); os.IsNotExist(err) {
		if err = os.MkdirAll(primaryDbPath, 0750); err != nil {
			panic(err)
		}
	}
	if _, err := os.Stat(secondaryDbPath); os.IsNotExist(err) {
		if err = os.MkdirAll(secondaryDbPath, 0750); err != nil {
			panic(err)
		}
	}
	return seh, primaryDbPath, secondaryDbPath
}

// 打印SEDB
func (sedb *SEDB) PrintSEDB() {
	fmt.Println("打印SEDB-----------------------------------------------------------------------")
	fmt.Printf("seHash:%s\n", hex.EncodeToString(sedb.seHash))
	fmt.Println("dbPath:", sedb.primaryDbPath)
	sedb.GetStorageEngine().PrintStorageEngine(sedb.primaryDb)
}
