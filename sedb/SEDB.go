package sedb

import (
	"MEHT/mbt"
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
//func (sedb *SEDB) InsertKVPair(kvPair *util.KVPair) *SEDBProof {}： 向SEDB中插入一条记录,返回插入证明
//func (sedb *SEDB) QueryKVPairsByHexKeyword(HexKeyword string) (string, []*util.KVPair, *SEDBProof) {}: 根据十六进制的非主键HexKeyword查询完整的kvPair
//func (sedb *SEDB) PrintKVPairsQueryResult(qKey string, qValue string, qResult []*util.KVPair, qProof *SEDBProof) {}: 打印非主键查询结果
//func (sedb *SEDB) VerifyQueryResult(pk string, result []*util.KVPair, sedbProof *SEDBProof) bool {}: 验证查询结果
//func (sedb *SEDB) WriteSEDBInfoToFile(filePath string) {}： 写seHash和dbPath到文件
//func ReadSEDBInfoFromFile(filePath string) ([]byte, string) {}： 从文件中读取seHash和dbPath
//func (sedb *SEDB) PrintSEDB() {}： 打印SEDB

type FullNodeCacheCapacity int                                  //MPT cache capacity identification for fullNode
type ShortNodeCacheCapacity int                                 //MPT cache capacity identification for shortNode
var DefaultFullNodeCacheCapacity = FullNodeCacheCapacity(128)   //MPT default cache capacity for shortNode
var DefaultShortNodeCacheCapacity = ShortNodeCacheCapacity(128) //MPT default cache capacity for fullNode

type MBTNodeCacheCapacity int                               //MBT cache capacity identification for mbtNode
type MBTBucketNum int                                       //MBT cache capacity identification for number of buckets
type MBTAggregation int                                     //MBT identification for aggregation size
var DefaultMBTNodeCacheCapacity = MBTNodeCacheCapacity(128) //MBT default cache capacity for mbtNode
var DefaultMBTBucketNum = MBTBucketNum(128)                 //MBT default number of buckets
var DefaultMBTAggregation = MBTAggregation(16)              //MBT default aggregation size

type MgtNodeCacheCapacity int                                    //MEHT cache capacity identification for mgtNode
type BucketCacheCapacity int                                     //MEHT cache capacity identification for bucket
type SegmentCacheCapacity int                                    //MEHT cache capacity identification for segment
type MerkleTreeCacheCapacity int                                 //MEHT cache capacity identification for merkleTree
type MEHTRdx int                                                 //meht的参数，meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
type MEHTBc int                                                  //meht的参数，meht中bucket的容量，即每个bucket中最多存储的KVPair数
type MEHTBs int                                                  //meht的参数，meht中segment划分的位数，即每个bucket中最多分割的segments数
var DefaultMgtNodeCacheCapacity = MgtNodeCacheCapacity(2 * 128)  //MEHT default cache capacity for mgtNode
var DefaultBucketCacheCapacity = BucketCacheCapacity(128)        //MEHT default cache capacity for bucket
var DefaultSegmentCacheCapacity = SegmentCacheCapacity(2 * 128)  //MEHT default cache capacity for segment
var DefaultMerkleTreeCapacity = MerkleTreeCacheCapacity(2 * 128) //MEHT default cache capacity for merkleTree
var DefaultMEHTRdx = MEHTRdx(16)                                 //MEHT default Rdx
var DefaultMEHTBc = MEHTBc(1280)                                 //MEHT default Bc
var DefaultMEHTBs = MEHTBs(1)                                    //MEHT default Bs

type SEDB struct {
	se              *StorageEngine //搜索引擎的指针
	seHash          []byte         //搜索引擎序列化后的哈希值
	primaryDb       *leveldb.DB    //主索引底层存储的指针
	secondaryDb     *leveldb.DB    //辅助索引底层存储的指针
	primaryDbPath   string         //主索引底层存储的文件路径
	secondaryDbPath string         //辅助索引底层存储的文件路径

	siMode        string        //se的参数，辅助索引类型，meht或mpt或mbt
	mbtArgs       []interface{} //mbt的参数，包含mbt的桶总数与分叉数
	mehtArgs      []interface{} //meht的参数，包含meht的分叉数、桶容量及段容量
	cacheEnable   bool          //是否使用缓存标识
	cacheCapacity []interface{} //缓存参数，包含各结构的缓存数目上界
	latch         sync.RWMutex  //当前结构体实例的全局锁
	updateLatch   sync.Mutex    //用于读锁升级为写锁
}

// NewSEDB 返回一个新的SEDB
func NewSEDB(seh []byte, primaryDbPath string, secondaryDbPath string, siMode string, mbtArgs []interface{}, mehtArgs []interface{}, cacheEnabled bool, cacheCapacity ...interface{}) *SEDB {
	//打开或创建数据库
	primaryDb, err := leveldb.OpenFile(primaryDbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	secondaryDb, err := leveldb.OpenFile(secondaryDbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	return &SEDB{nil, seh, primaryDb, secondaryDb, primaryDbPath, secondaryDbPath,
		siMode, mbtArgs, mehtArgs,
		cacheEnabled, cacheCapacity, sync.RWMutex{}, sync.Mutex{}}
}

// GetStorageEngine 获取SEDB中的StorageEngine，如果为空，从db中读取se
func (sedb *SEDB) GetStorageEngine() *StorageEngine {
	//如果se为空，从db中读取se
	if sedb.se == nil && sedb.seHash != nil && len(sedb.seHash) != 0 && sedb.latch.TryLock() { // 只允许一个线程重构se
		if sedb.se != nil { //可能在在TryLock之前刚好有se已经被从磁盘中读取并重构，因此需要判断是否还需要重构se
			sedb.latch.Unlock()
			return sedb.se
		}
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

// InsertKVPair 向SEDB中插入一条记录,返回插入证明
func (sedb *SEDB) InsertKVPair(kvPair util.KVPair) *SEDBProof {
	//如果是第一次插入
	for sedb.GetStorageEngine() == nil && sedb.latch.TryLock() { // 只允许一个线程新建se
		if sedb.se != nil { //可能在在TryLock之前刚好有se已经被实例化，因此需要判断是否还需要创建se
			sedb.latch.Unlock()
			break
		}
		//创建一个新的StorageEngine
		sedb.se = NewStorageEngine(sedb.siMode, sedb.mbtArgs, sedb.mehtArgs, sedb.cacheEnable,
			sedb.cacheCapacity)
		sedb.latch.Unlock()
	}
	for sedb.se == nil {
	} // 其余线程等待se创建
	//向StorageEngine中插入一条记录
	//primaryProof, secondaryMPTProof, secondaryMEHTProof := sedb.GetStorageEngine().Insert(kvPair, sedb.db)
	sedb.GetStorageEngine().Insert(kvPair, sedb.primaryDb, sedb.secondaryDb)
	//更新seHash，并将se更新至db
	//sedb.se.UpdateStorageEngineToDB(sedb.primaryDb) // 保证sedb留存的seHash与se实际的Hash一致
	//sedb.updateLatch.Lock()
	//sedb.se.updatePrimaryLatch.Lock()
	//sedb.se.updateSecondaryLatch.Lock()
	//sedb.seHash = sedb.se.seHash
	//sedb.se.updateSecondaryLatch.Unlock()
	//sedb.se.updatePrimaryLatch.Unlock()
	//sedb.updateLatch.Unlock()
	//var pProof []*mpt.MPTProof
	//pProof = append(pProof, primaryProof)
	//sedbProof := NewSEDBProof(pProof, secondaryMPTProof, secondaryMEHTProof)
	//返回插入结果
	//return sedbProof
	return nil
}

// 使用管道向工作线程分发查询工作所需key
func generatePrimaryKey(primaryKeys []string, primaryKeyCh chan string) {
	for _, key := range primaryKeys {
		primaryKeyCh <- key
	}
	close(primaryKeyCh)
}

// 新建工作线程池
func createWorkerPool(numOfWorker int, sedb *SEDB, primaryKeyCh chan string, lock *sync.Mutex, queryResult *[]*util.KVPair, primaryProof *[]*mpt.MPTProof) {
	wg := sync.WaitGroup{}
	wg.Add(numOfWorker)
	for i := 0; i < numOfWorker; i++ {
		go workerForPrimarySearch(&wg, sedb, primaryKeyCh, lock, queryResult, primaryProof)
	}
	wg.Wait()
}

// 定义工作线程的具体工作事务
func workerForPrimarySearch(wg *sync.WaitGroup, sedb *SEDB, primaryKeyCh chan string, lock *sync.Mutex, queryResult *[]*util.KVPair, primaryProof *[]*mpt.MPTProof) {
	for primaryKey := range primaryKeyCh { //等待工作分发，直至管道被关闭，线程结束
		qV, pProof := sedb.GetStorageEngine().GetPrimaryIndex(sedb.primaryDb).QueryByKey(primaryKey, sedb.primaryDb, false)
		//用qV和primaryKeys[i]构造一个kvPair
		kvPair := util.NewKVPair(primaryKey, qV)
		lock.Lock()
		//把kvPair加入queryResult
		*queryResult = append(*queryResult, kvPair)
		//把pProof加入primaryProof
		*primaryProof = append(*primaryProof, pProof)
		lock.Unlock()
	}
	wg.Done()
}

// QueryKVPairsByHexKeyword 根据十六进制的非主键HexKeyword查询完整的kvPair
func (sedb *SEDB) QueryKVPairsByHexKeyword(HexKeyword string) (string, []*util.KVPair, *SEDBProof) {
	if sedb.GetStorageEngine() == nil {
		//fmt.Println("SEDB is empty!")
		return "", nil, nil
	}
	//根据HexKeyword在非主键索引中查询
	var primaryKey string
	var secondaryMPTProof *mpt.MPTProof
	var secondaryMBTProof *mbt.MBTProof
	var secondaryMEHTProof *meht.MEHTProof
	var primaryProof []*mpt.MPTProof
	var queryResult []*util.KVPair
	var lock sync.Mutex
	primaryKeyCh := make(chan string)
	if sedb.siMode == "mpt" {
		primaryKey, secondaryMPTProof = sedb.GetStorageEngine().GetSecondaryIndexMpt(sedb.secondaryDb).QueryByKey(HexKeyword, sedb.secondaryDb, false)
		//根据primaryKey在主键索引中查询
		if primaryKey == "" {
			//sum++
			//fmt.Println("No such key in mpt!", "     ", sum)
			return "", nil, NewSEDBProof(nil, secondaryMPTProof, secondaryMBTProof, secondaryMEHTProof)
		}
		primaryKeys := strings.Split(primaryKey, ",")
		go generatePrimaryKey(primaryKeys, primaryKeyCh)
		createWorkerPool(len(primaryKeys)/2+1, sedb, primaryKeyCh, &lock, &queryResult, &primaryProof)
		return primaryKey, queryResult, NewSEDBProof(primaryProof, secondaryMPTProof, secondaryMBTProof, secondaryMEHTProof)
	} else if sedb.siMode == "meht" {
		pKey, qBucket, segKey, isSegExist, index := sedb.GetStorageEngine().GetSecondaryIndexMeht(sedb.secondaryDb).QueryValueByKey(HexKeyword, sedb.secondaryDb)
		primaryKey = pKey
		//根据primaryKey在主键索引中查询，同时构建MEHT的查询证明
		ch := make(chan *meht.MEHTProof)
		go func(ch chan *meht.MEHTProof) {
			seMEHTProof := sedb.GetStorageEngine().GetSecondaryIndexMeht(sedb.secondaryDb).GetQueryProof(qBucket, segKey, isSegExist, index, sedb.secondaryDb)
			ch <- seMEHTProof
		}(ch)
		//根据primaryKey在主键索引中查询
		if primaryKey == "" {
			//sum++
			//fmt.Println("No such key in meht!", "     ", sum)
		} else {
			primaryKeys := strings.Split(primaryKey, ",")
			go generatePrimaryKey(primaryKeys, primaryKeyCh)
			createWorkerPool(len(primaryKeys)/2+1, sedb, primaryKeyCh, &lock, &queryResult, &primaryProof)
		}
		secondaryMEHTProof = <-ch
		return primaryKey, queryResult, NewSEDBProof(primaryProof, secondaryMPTProof, secondaryMBTProof, secondaryMEHTProof)
	} else if sedb.siMode == "mbt" {
		mbtIndex := sedb.GetStorageEngine().GetSecondaryIndexMbt(sedb.secondaryDb)
		path := mbt.ComputePath(mbtIndex.GetBucketNum(), mbtIndex.GetAggregation(), mbtIndex.GetGd(), HexKeyword)
		primaryKey, secondaryMBTProof = mbtIndex.QueryByKey(HexKeyword, path, sedb.secondaryDb, false)
		//根据primaryKey在主键索引中查询
		if primaryKey == "" {
			return "", nil, NewSEDBProof(nil, secondaryMPTProof, secondaryMBTProof, secondaryMEHTProof)
		}
		primaryKeys := strings.Split(primaryKey, ",")
		go generatePrimaryKey(primaryKeys, primaryKeyCh)
		createWorkerPool(len(primaryKeys)/2+1, sedb, primaryKeyCh, &lock, &queryResult, &primaryProof)
		return primaryKey, queryResult, NewSEDBProof(primaryProof, secondaryMPTProof, secondaryMBTProof, secondaryMEHTProof)
	} else {
		fmt.Println("siMode is wrong!")
		return "", nil, nil
	}
}

// PrintKVPairsQueryResult 打印非主键查询结果
func (sedb *SEDB) PrintKVPairsQueryResult(qKey string, qValue string, qResult []*util.KVPair, qProof *SEDBProof) {
	fmt.Printf("打印查询结果-------------------------------------------------------------------------------------------\n")
	fmt.Printf("查询关键字为%s的主键为:%s\n", qKey, qValue)
	for i := 0; i < len(qResult); i++ {
		fmt.Printf("查询结果[%d]:key=%s,value=%s\n", i, util.HexToString(qResult[i].GetKey()), util.HexToString(qResult[i].GetValue()))
	}
	//fmt.Printf("查询证明为:\n")
	if qProof != nil {
		//qProof.PrintSEDBProof()
	} else {
		fmt.Printf("查询证明为空\n")
	}
}

// VerifyQueryResult 验证查询结果
func (sedb *SEDB) VerifyQueryResult(pk string, result []*util.KVPair, sedbProof *SEDBProof) bool {
	r := false
	rCh := make(chan bool)
	fmt.Printf("验证查询结果-------------------------------------------------------------------------------------------\n")
	//验证非主键查询结果
	fmt.Printf("验证非主键查询结果:")
	if sedb.siMode == "mpt" {
		r = sedb.se.GetSecondaryIndexMpt(sedb.secondaryDb).VerifyQueryResult(pk, sedbProof.GetSecondaryMPTIndexProof())
	} else if sedb.siMode == "meht" {
		r = meht.VerifyQueryResult(pk, sedbProof.GetSecondaryMEHTIndexProof())
	} else if sedb.siMode == "mbt" {
		r = sedb.se.GetSecondaryIndexMbt(sedb.secondaryDb).VerifyQueryResult(pk, sedbProof.GetSecondaryMBTIndexProof())
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

// WriteSEDBInfoToFile 写seHash和dbPath到文件
func (sedb *SEDB) WriteSEDBInfoToFile(filePath string) {
	se := sedb.GetStorageEngine()
	se.UpdateStorageEngineToDB()
	sedb.seHash = se.seHash
	if err := sedb.primaryDb.Put(se.seHash, SerializeStorageEngine(se), nil); err != nil {
		fmt.Println("Insert StorageEngine to DB error:", err)
	}
	if sedb.cacheEnable {
		sedb.GetStorageEngine().GetPrimaryIndex(sedb.primaryDb).PurgeCache()
		switch sedb.siMode {
		case "meht":
			sedb.GetStorageEngine().GetSecondaryIndexMeht(sedb.secondaryDb).PurgeCache()
			se.GetSecondaryIndexMeht(sedb.secondaryDb).GetSEH(sedb.secondaryDb).UpdateSEHToDB(sedb.secondaryDb)
		case "mpt":
			sedb.GetStorageEngine().GetSecondaryIndexMpt(sedb.secondaryDb).PurgeCache()
		case "mbt":
			sedb.GetStorageEngine().GetSecondaryIndexMbt(sedb.secondaryDb).PurgeCache()
		default:
			log.Fatal("Unknown siMode when purge cache.")
		}
	}
	data := hex.EncodeToString(sedb.seHash) + "," + sedb.primaryDbPath + "," + sedb.secondaryDbPath + "\n"
	util.WriteStringToFile(filePath, data)
}

// ReadSEDBInfoFromFile 从文件中读取seHash和dbPath
func ReadSEDBInfoFromFile(filePath string) ([]byte, string, string) {
	data, _ := util.ReadStringFromFile(filePath)
	sehDbpath := strings.Split(data, ",")
	if len(sehDbpath) != 3 {
		fmt.Println("seHash and dbPath don't exist!")
		return nil, "", ""
	}
	seh, _ := hex.DecodeString(sehDbpath[0])
	primaryDbPath := util.Strip(sehDbpath[1], "\n\t\r")
	secondaryDbPath := util.Strip(sehDbpath[2], "\n\t\r")
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

// PrintSEDB 打印SEDB
func (sedb *SEDB) PrintSEDB() {
	fmt.Println("打印SEDB-----------------------------------------------------------------------")
	fmt.Printf("seHash:%s\n", hex.EncodeToString(sedb.seHash))
	fmt.Println("dbPath:", sedb.primaryDbPath)
	sedb.GetStorageEngine().PrintStorageEngine(sedb.primaryDb)
}
