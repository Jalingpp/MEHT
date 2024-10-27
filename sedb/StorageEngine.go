package sedb

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/Jalingpp/MEST/mbt"
	"github.com/Jalingpp/MEST/meht"
	"github.com/Jalingpp/MEST/mpt"
	"github.com/Jalingpp/MEST/util"

	"github.com/syndtr/goleveldb/leveldb"
)

type StorageEngine struct {
	seHash                 [32]byte //搜索引擎的哈希值，由主索引根哈希和辅助索引根哈希计算得到
	primaryIndex           *mpt.MPT // mpt类型的主键索引
	primaryIndexHash       [32]byte // 主键索引的哈希值，由主键索引根哈希计算得到
	secondaryIndexMode     string   // 标识当前采用的非主键索引的类型，mpt或meht或mbt
	secondaryIndexMpt      *mpt.MPT // mpt类型的非主键索引
	secondaryIndexHashMpt  [32]byte //mpt类型的非主键索引根哈希
	secondaryIndexMbt      *mbt.MBT
	secondaryIndexHashMbt  [32]byte
	secondaryIndexMeht     *meht.MEHT // meht类型的非主键索引，在db中用mehtName+"meht"索引
	secondaryIndexHashMeht [32]byte
	mbtArgs                []interface{}
	mehtArgs               []interface{}
	cacheEnable            bool
	cacheCapacity          []interface{}
	primaryLatch           sync.RWMutex
	secondaryLatch         sync.RWMutex
	updatePrimaryLatch     sync.Mutex
	updateSecondaryLatch   sync.Mutex
}

// NewStorageEngine 返回一个新的StorageEngine
func NewStorageEngine(siMode string, mbtArgs []interface{}, mehtArgs []interface{},
	cacheEnable bool, cacheCapacity []interface{}) *StorageEngine {
	return &StorageEngine{[32]byte{}, nil, [32]byte{}, siMode, nil,
		[32]byte{}, nil, [32]byte{}, nil, [32]byte{}, mbtArgs, mehtArgs,
		cacheEnable, cacheCapacity, sync.RWMutex{}, sync.RWMutex{},
		sync.Mutex{}, sync.Mutex{}}
}

// UpdateStorageEngineToDB 更新存储引擎的哈希值，并将更新后的存储引擎写入db中
func (se *StorageEngine) UpdateStorageEngineToDB() {
	//更新seHash的哈希值
	seHashes := make([]byte, 0)
	se.updatePrimaryLatch.Lock()
	se.updateSecondaryLatch.Lock() // 保证se的哈希与辅助索引根哈希是相关联的
	seHashes = append(seHashes, se.primaryIndexHash[:]...)
	if se.secondaryIndexMode == "mpt" {
		seHashes = append(seHashes, se.secondaryIndexHashMpt[:]...)
	} else if se.secondaryIndexMode == "meht" {
		seHashes = append(seHashes, se.secondaryIndexHashMeht[:]...)
	} else if se.secondaryIndexMode == "mbt" {
		seHashes = append(seHashes, se.secondaryIndexHashMbt[:]...)
	} else {
		fmt.Printf("非主键索引类型siMode设置错误\n")
	}
	se.seHash = sha256.Sum256(seHashes)
	se.updateSecondaryLatch.Unlock()
	se.updatePrimaryLatch.Unlock()
}

// GetPrimaryIndex 返回主索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetPrimaryIndex(db *leveldb.DB) *mpt.MPT {
	//如果当前primaryIndex为空，则从数据库中查询
	if se.primaryIndex == nil && se.primaryIndexHash != [32]byte{} && se.primaryLatch.TryLock() { // 只允许一个线程重构主索引
		if se.primaryIndex != nil {
			se.primaryLatch.Unlock()
			return se.primaryIndex
		}
		if primaryIndexString, error_ := db.Get(se.primaryIndexHash[:], nil); error_ == nil {
			shortNodeCC, fullNodeCC, _, _, _, _, _ := GetCapacity(&se.cacheCapacity)
			primaryIndex, _ := mpt.DeserializeMPT(primaryIndexString, db, se.cacheEnable, shortNodeCC, fullNodeCC)
			se.primaryIndex = primaryIndex
		} else {
			fmt.Printf("GetPrimaryIndex error:%v\n", error_)
		}
		se.primaryLatch.Unlock()
	}
	for se.primaryIndex == nil && se.primaryIndexHash != [32]byte{} {
	} // 其余线程等待主索引重构
	return se.primaryIndex
}

// GetSecondaryIndexMpt 返回mpt类型的辅助索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetSecondaryIndexMpt(db *leveldb.DB) *mpt.MPT {
	//如果当前secondaryIndex_mpt为空，则从数据库中查询
	if se.secondaryIndexMpt == nil && se.secondaryIndexHashMpt != [32]byte{} && se.secondaryLatch.TryLock() {
		if se.secondaryIndexMpt != nil {
			se.secondaryLatch.Unlock()
			return se.secondaryIndexMpt
		}
		if secondaryIndexString, _ := db.Get(se.secondaryIndexHashMpt[:], nil); len(secondaryIndexString) != 0 {
			shortNodeCC, fullNodeCC, _, _, _, _, _ := GetCapacity(&se.cacheCapacity)
			secondaryIndex, _ := mpt.DeserializeMPT(secondaryIndexString, db, se.cacheEnable, shortNodeCC, fullNodeCC)
			se.secondaryIndexMpt = secondaryIndex
		}
		se.secondaryLatch.Unlock()
	}
	for se.secondaryIndexMpt == nil && se.secondaryIndexHashMpt != [32]byte{} {
	}
	return se.secondaryIndexMpt
}

// GetSecondaryIndexMbt 返回mbt类型的辅助索引指针，如果内存中不存在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetSecondaryIndexMbt(db *leveldb.DB) *mbt.MBT {
	//如果当前secondaryIndex_mbt为空，则从数据库中查询
	if se.secondaryIndexMbt == nil && se.secondaryIndexHashMbt != [32]byte{} && se.secondaryLatch.TryLock() {
		if se.secondaryIndexMbt != nil {
			se.secondaryLatch.Unlock()
			return se.secondaryIndexMbt
		}
		if secondaryIndexString, _ := db.Get(se.secondaryIndexHashMbt[:], nil); len(secondaryIndexString) != 0 {
			_, _, mbtNodeCC, _, _, _, _ := GetCapacity(&se.cacheCapacity)
			secondaryIndex, _ := mbt.DeserializeMBT(secondaryIndexString, db, se.cacheEnable, mbtNodeCC)
			se.secondaryIndexMbt = secondaryIndex
		}
		se.secondaryLatch.Unlock()
	}
	for se.secondaryIndexMbt == nil && se.secondaryIndexHashMbt != [32]byte{} {
	}
	return se.secondaryIndexMbt
}

// GetSecondaryIndexMeht 返回meht类型的辅助索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetSecondaryIndexMeht(db *leveldb.DB) *meht.MEHT {
	//如果当前secondaryIndex_meht为空，则从数据库中查询，如果数据库中也找不到，则在纯查询空meht场景下会有大问题
	if se.secondaryIndexMeht == nil && se.secondaryIndexHashMeht != [32]byte{} && se.secondaryLatch.TryLock() {
		if se.secondaryIndexMeht != nil {
			se.secondaryLatch.Unlock()
			return se.secondaryIndexMeht
		}
		if secondaryIndexString, _ := db.Get(se.secondaryIndexHashMeht[:], nil); len(secondaryIndexString) != 0 {
			_, _, _, mgtNodeCC, bucketCC, segmentCC, merkleTreeCC := GetCapacity(&se.cacheCapacity)
			se.secondaryIndexMeht, _ = meht.DeserializeMEHT(secondaryIndexString, db, se.cacheEnable, mgtNodeCC,
				bucketCC, segmentCC, merkleTreeCC)
			se.secondaryIndexMeht.GetMGT(db).GetRoot(db) // 保证从磁盘读取MEHT成功后MGT也被同步从磁盘中读取出
		}
		se.secondaryLatch.Unlock()
	}
	for se.secondaryIndexMeht == nil && se.secondaryIndexHashMeht != [32]byte{} {
	}
	return se.secondaryIndexMeht
}

// Insert 向StorageEngine中插入一条记录,返回插入后新的seHash，以及插入的证明
func (se *StorageEngine) Insert(kvPair util.KVPair, isUpdate bool, primaryDb *leveldb.DB, secondaryDb *leveldb.DB) (*mpt.MPTProof, *mpt.MPTProof, *meht.MEHTProof) {
	if se.secondaryIndexMode != "meht" && se.secondaryIndexMode != "mpt" && se.secondaryIndexMode != "mbt" {
		fmt.Printf("非主键索引类型siMode设置错误\n")
		return nil, nil, nil
	}
	//插入主键索引
	//如果是第一次插入
	if se.GetPrimaryIndex(primaryDb) == nil && se.primaryLatch.TryLock() { // 主索引重构失败，只允许一个线程新建主索引
		//创建一个新的主键索引
		if se.GetPrimaryIndex(primaryDb) != nil {
			se.primaryLatch.Unlock()
		} else {
			shortNodeCC, fullNodeCC, _, _, _, _, _ := GetCapacity(&se.cacheCapacity)
			se.primaryIndex = mpt.NewMPT(primaryDb, se.cacheEnable, shortNodeCC, fullNodeCC)
			se.primaryLatch.Unlock()
		}
	}
	for se.primaryIndex == nil {
	} // 其余线程等待主索引新建成功
	//如果主索引中已存在此key，则获取原来的value，并在非主键索引中删除该value-key对
	var wG sync.WaitGroup
	wG.Add(2)
	oldValueCh := make(chan string)
	needDeleteCh := make(chan bool)
	go func(isUpdate bool) {
		defer wG.Done()
		//将KV插入到主键索引中
		oldVal, needDelete := se.primaryIndex.Insert(kvPair, primaryDb, nil, isUpdate)
		if isUpdate {
			oldValueCh <- oldVal
			needDeleteCh <- needDelete
		}
		se.updatePrimaryLatch.Lock() // 保证se留存的主索引哈希与实际主索引根哈希一致
		se.primaryIndex.GetUpdateLatch().Lock()
		rootHash := se.primaryIndex.GetRootHash()
		se.primaryIndexHash = sha256.Sum256(rootHash[:])
		se.primaryIndex.GetUpdateLatch().Unlock()
		se.updatePrimaryLatch.Unlock()
		//_, primaryProof := se.GetPrimaryIndex(db).QueryByKey(kvPair.GetKey(), db)
		//fmt.Printf("key=%x , value=%x已插入主键索引MPT\n", []byte(kvPair.GetKey()), []byte(kvPair.GetValue()))
	}(isUpdate)
	//构造倒排KV
	reversedKV := util.ReverseKVPair(kvPair)
	var oldVal string
	var needDelete bool
	//插入非主键索引
	if se.secondaryIndexMode == "mpt" { //插入mpt
		//插入到mpt类型的非主键索引中
		//_, mptProof := se.InsertIntoMPT(reversedKV, db)
		go func(isUpdate bool) { // 实际应该为返回值mptProof构建一个chan并等待输出
			defer wG.Done()
			se.InsertIntoMPT(reversedKV, secondaryDb, se.primaryIndex, false)
			if isUpdate {
				oldVal = <-oldValueCh
				needDelete = <-needDeleteCh
			}
			if needDelete {
				se.InsertIntoMPT(*util.NewKVPair(oldVal, reversedKV.GetValue()), secondaryDb, se.primaryIndex, true)
			}
		}(isUpdate)
		//更新搜索引擎的哈希值
		//se.UpdateStorageEngineToDB(db)
		//return primaryProof, mptProof, nil
	} else if se.secondaryIndexMode == "meht" { //插入meht
		//var mehtProof *meht.MEHTProof
		//_, mehtProof  = se.InsertIntoMEHT(reversedKV, db)
		go func(isUpdate bool) { //实际应该为返回值mehtProof构建一个chan并等待输出
			defer wG.Done()
			se.InsertIntoMEHT(reversedKV, secondaryDb, false)
			if isUpdate {
				oldVal = <-oldValueCh
				needDelete = <-needDeleteCh
			}
			if needDelete {
				se.InsertIntoMEHT(*util.NewKVPair(oldVal, reversedKV.GetValue()), secondaryDb, true)
			}
		}(isUpdate)
		//更新搜索引擎的哈希值
		//se.UpdateStorageEngineToDB(db)
		//return primaryProof, nil, mehtProof
	} else {
		go func(isUpdate bool) {
			defer wG.Done()
			se.InsertIntoMBT(reversedKV, secondaryDb, false)
			if isUpdate {
				oldVal = <-oldValueCh
				needDelete = <-needDeleteCh
			}
			if needDelete {
				se.InsertIntoMBT(*util.NewKVPair(oldVal, reversedKV.GetValue()), secondaryDb, true)
			}
		}(isUpdate)
	}
	wG.Wait()
	return nil, nil, nil
}

func (se *StorageEngine) BatchCommit(primaryDb *leveldb.DB, secondaryDb *leveldb.DB) {
	if se.secondaryIndexMode != "meht" && se.secondaryIndexMode != "mpt" && se.secondaryIndexMode != "mbt" {
		fmt.Printf("非主键索引类型siMode设置错误\n")
		return
	}
	wG := sync.WaitGroup{}
	wG.Add(2)
	go func() {
		se.PrimaryIndexBatchCommit(primaryDb)
		wG.Done()
	}()
	go func() {
		if se.secondaryIndexMode == "mpt" { //插入mpt
			se.MPTBatchCommit(secondaryDb)
		} else if se.secondaryIndexMode == "meht" { //插入meht
			se.MEHTBatchCommit(secondaryDb)
		} else if se.secondaryIndexMode == "mbt" {
			se.MBTBatchCommit(secondaryDb)
		}
		wG.Done()
	}()
	wG.Wait()
	return
}

func (se *StorageEngine) CacheAdjust(secondaryDb *leveldb.DB, a float64, b float64) {
	if se.secondaryIndexMode != "meht" && se.secondaryIndexMode != "mpt" && se.secondaryIndexMode != "mbt" {
		fmt.Printf("非主键索引类型siMode设置错误\n")
		return
	}
	if se.primaryIndex == nil {
		return
	}
	if se.secondaryIndexMode == "mpt" { //mpt无缓存调整
		return
	} else if se.secondaryIndexMode == "meht" {
		se.MEHTCacheAdjust(secondaryDb, a, b)
	} else if se.secondaryIndexMode == "mbt" { //mbt无缓存调整
		return
	}
	return
}

func (se *StorageEngine) PrimaryIndexBatchCommit(db *leveldb.DB) {
	if se.primaryIndex == nil {
		return
	}
	se.primaryIndex.MPTBatchFix(db)
	rootHash := se.primaryIndex.GetRootHash()
	se.primaryIndexHash = sha256.Sum256(rootHash[:])
}

// MPTBatchCommit 批量提交mpt
func (se *StorageEngine) MPTBatchCommit(db *leveldb.DB) {
	if se.secondaryIndexMpt == nil { // 没有辅助索引说明没有交易需要批量提交
		return
	}
	// 批量更新mgt，正式提交
	se.secondaryIndexMpt.MPTBatchFix(db)
	// 批量更新会更新mpt的rootHash,因此需要更新se的辅助索引根哈希
	rootHash := se.secondaryIndexMpt.GetRootHash()
	se.secondaryIndexHashMpt = sha256.Sum256(rootHash[:])
}

// InsertIntoMPT 插入非主键索引
func (se *StorageEngine) InsertIntoMPT(kvPair util.KVPair, db *leveldb.DB, priMPT *mpt.MPT, flag bool) (string, *mpt.MPTProof) {
	//如果是第一次插入
	if se.GetSecondaryIndexMpt(db) == nil && se.secondaryLatch.TryLock() { // 总有一个线程会拿到写锁并创建非主键索引
		//创建一个新的非主键索引
		shortNodeCC, fullNodeCC, _, _, _, _, _ := GetCapacity(&se.cacheCapacity)
		se.secondaryIndexMpt = mpt.NewMPT(db, se.cacheEnable, shortNodeCC, fullNodeCC)
		se.secondaryLatch.Unlock()
	}
	for se.secondaryIndexMpt == nil { // 其余线程等待非主键索引创建
	}
	//先查询得到原有value与待插入value合并
	//_, mptProof := se.secondaryIndexMpt.QueryByKey(kvPair.GetKey(), db, false)
	se.secondaryIndexMpt.Insert(kvPair, db, priMPT, flag)
	//newValues, newProof := se.secondaryIndex_mpt.QueryByKey(insertedKV.GetKey(), db)
	//return newValues, newProof
	return "", nil
}

// MBTBatchCommit 批量提交mbt
func (se *StorageEngine) MBTBatchCommit(db *leveldb.DB) {
	if se.secondaryIndexMbt == nil { // 没有辅助索引说明没有交易需要批量提交
		return
	}
	// 批量更新mgt，正式提交
	se.secondaryIndexMbt.MBTBatchFix(db)
	// 批量更新会更新mbt的rootHash,因此需要更新se的辅助索引根哈希
	rootHash := se.secondaryIndexMbt.GetRootHash()
	se.secondaryIndexHashMbt = sha256.Sum256(rootHash[:])
}

// InsertIntoMBT 插入非主键索引
func (se *StorageEngine) InsertIntoMBT(kvPair util.KVPair, db *leveldb.DB, isDelete bool) (string, *mbt.MBTProof) {
	//如果是第一次插入
	if se.GetSecondaryIndexMbt(db) == nil && se.secondaryLatch.TryLock() { // 总有一个线程会拿到写锁并创建非主键索引
		//创建一个新的非主键索引
		_, _, mbtNodeCC, _, _, _, _ := GetCapacity(&se.cacheCapacity)
		bucketNum, aggregation := GetMBTArgs(&se.mbtArgs)
		se.secondaryIndexMbt = mbt.NewMBT(bucketNum, aggregation, db, se.cacheEnable, mbtNodeCC)
		se.secondaryLatch.Unlock()
	}
	for se.secondaryIndexMbt == nil { // 其余线程等待非主键索引创建
	}
	//先查询得到原有value与待插入value合并
	//path := mbt.ComputePath(se.secondaryIndexMbt.GetBucketNum(), se.secondaryIndexMbt.GetAggregation(), se.secondaryIndexMbt.GetGd(), kvPair.GetKey())
	//values, mbtProof := se.secondaryIndexMbt.QueryByKey(kvPair.GetKey(), path, db, false)
	se.secondaryIndexMbt.Insert(kvPair, db, isDelete)
	return "", nil
	//return values, mbtProof
}

func (se *StorageEngine) MEHTBatchCommit(db *leveldb.DB) {
	if se.secondaryIndexMeht == nil { // 没有辅助索引说明没有交易需要批量提交
		return
	}
	// 批量更新mgt，正式提交
	se.secondaryIndexMeht.MGTBatchCommit(db)
	// 批量更新会更新mgt的rootHash,因此需要更新se的辅助索引根哈希,已经做过sha256
	rootHash := se.secondaryIndexMeht.GetMgtHash()
	se.secondaryIndexHashMeht = sha256.Sum256(rootHash[:])
}

func (se *StorageEngine) MEHTCacheAdjust(db *leveldb.DB, a float64, b float64) {
	if se.secondaryIndexMeht == nil {
		return
	}
	se.secondaryIndexMeht.MGTCacheAdjust(db, a, b)
	rootHash := se.secondaryIndexMeht.GetMgtHash()
	se.secondaryIndexHashMeht = sha256.Sum256(rootHash[:])
}

// InsertIntoMEHT 插入非主键索引
func (se *StorageEngine) InsertIntoMEHT(kvPair util.KVPair, db *leveldb.DB, isDelete bool) (string, *meht.MEHTProof) {
	//如果是第一次插入
	_, _, _, _, _, _, _, isBF := GetMEHTArgs(&se.mehtArgs)
	if se.GetSecondaryIndexMeht(db) == nil && se.secondaryLatch.TryLock() { // 总有一个线程会获得锁并创建meht
		//创建一个新的非主键索引
		_, _, _, mgtNodeCC, bucketCC, segmentCC, merkleTreeCC := GetCapacity(&se.cacheCapacity)
		mehtRdx, mehtBc, mehtBs, mehtWs, mehtSt, mehtBFsize, mehtBFhnum, _ := GetMEHTArgs(&se.mehtArgs)
		se.secondaryIndexMeht = meht.NewMEHT(mehtRdx, mehtBc, mehtBs, mehtWs, mehtSt, mehtBFsize, mehtBFhnum, db, mgtNodeCC, bucketCC, segmentCC, merkleTreeCC, se.cacheEnable)
		// isBF = mehtIsbf
		se.secondaryLatch.Unlock()
	}
	for se.GetSecondaryIndexMeht(db) == nil { // 其余线程等待meht创建成功
	}
	// 这里逻辑也需要转变，因为并发插入的时候可能很多键都相同但被阻塞了一直没写进去，那更新就会有非常多初始值的重复
	// 因此这里不先进行与初始值的合并，而是在后续委托插入的时候进行重复键的值合并，然后一并插入到桶里的时候利用map结构再对插入值与初始值进行合并去重
	//_, newValues, newProof := se.secondaryIndex_meht.Insert(insertedKV, db)
	// fmt.Println(isBF)
	_, newValues, newProof := se.secondaryIndexMeht.Insert(kvPair, db, isDelete, isBF)
	//本来是需要更新meht到db，但是现在是批量插入，mgtHash是脏的，所以更新没有意义
	//se.secondaryIndexMeht.UpdateMEHTToDB(db)
	return newValues, newProof
}

// GetMBTArgs 解析MBTArgs
func GetMBTArgs(mbtArgs *[]interface{}) (mbtBucketNum int, mbtAggregation int) {
	mbtBucketNum = int(DefaultMBTBucketNum)
	mbtAggregation = int(DefaultMBTAggregation)
	for _, arg := range *mbtArgs {
		switch arg.(type) {
		case MBTBucketNum:
			mbtBucketNum = int(arg.(MBTBucketNum))
		case MBTAggregation:
			mbtAggregation = int(arg.(MBTAggregation))
		default:
			panic("Unknown type " + reflect.TypeOf(arg).String() + " in function GetMBTArgs.")
		}
	}
	return
}

// GetMEHTArgs 解析MEHTArgs
func GetMEHTArgs(mehtArgs *[]interface{}) (mehtRdx int, mehtBc int, mehtBs int, mehtWs int, mehtSt int, mehtBFsize int, mehtBFhnum int, mehtIsbf bool) {
	mehtRdx = int(DefaultMEHTRdx)
	mehtBc = int(DefaultMEHTBc)
	mehtBs = int(DefaultMEHTBs)
	mehtWs = int(DefaultMEHTWs)
	mehtSt = int(DefaultMEHTSt)
	mehtBFsize = int(DefaultMEHTBFsize)
	mehtBFhnum = int(DefaultMEHTBFHnum)
	mehtIsbf = bool(DefaultMEHTIsBF)
	for _, arg := range *mehtArgs {
		switch arg.(type) {
		case MEHTRdx:
			mehtRdx = int(arg.(MEHTRdx))
		case MEHTBc:
			mehtBc = int(arg.(MEHTBc))
		case MEHTBs:
			mehtBs = int(arg.(MEHTBs))
		case MEHTWs:
			mehtWs = int(arg.(MEHTWs))
		case MEHTSt:
			mehtSt = int(arg.(MEHTSt))
		case MEHTBFsize:
			mehtBFsize = int(arg.(MEHTBFsize))
		case MEHTBFHnum:
			mehtBFhnum = int(arg.(MEHTBFHnum))
		case MEHTIsBF:
			mehtIsbf = bool(arg.(MEHTIsBF))
		default:
			fmt.Println("arg", arg)
			panic("Unknown type " + reflect.TypeOf(arg).String() + " in function GetMEHTArgs.")
		}
	}
	return
}

// GetCapacity 解析缓存大小参数
func GetCapacity(cacheCapacity *[]interface{}) (shortNodeCC int, fullNodeCC int, mbtNodeCC int, mgtNodeCC int, bucketCC int, segmentCC int, merkleTreeCC int) {
	shortNodeCC = int(DefaultShortNodeCacheCapacity)
	fullNodeCC = int(DefaultFullNodeCacheCapacity)
	mbtNodeCC = int(DefaultMBTNodeCacheCapacity)
	mgtNodeCC = int(DefaultMgtNodeCacheCapacity)
	bucketCC = int(DefaultBucketCacheCapacity)
	segmentCC = int(DefaultSegmentCacheCapacity)
	merkleTreeCC = int(DefaultMerkleTreeCapacity)
	cacheCapacity_ := *cacheCapacity
	for _, capacity := range cacheCapacity_ {
		switch capacity.(type) {
		case ShortNodeCacheCapacity:
			shortNodeCC = int(capacity.(ShortNodeCacheCapacity))
		case FullNodeCacheCapacity:
			fullNodeCC = int(capacity.(FullNodeCacheCapacity))
		case MBTNodeCacheCapacity:
			mbtNodeCC = int(capacity.(MBTNodeCacheCapacity))
		case MgtNodeCacheCapacity:
			mgtNodeCC = int(capacity.(MgtNodeCacheCapacity))
		case BucketCacheCapacity:
			bucketCC = int(capacity.(BucketCacheCapacity))
		case SegmentCacheCapacity:
			segmentCC = int(capacity.(SegmentCacheCapacity))
		case MerkleTreeCacheCapacity:
			merkleTreeCC = int(capacity.(MerkleTreeCacheCapacity))
		default:
			panic("Unknown type " + reflect.TypeOf(capacity).String() + " in function GetCapacity.")
		}
	}
	return
}

//// PrintQueryResult 打印查询结果
//func PrintQueryResult(key string, value string, mptProof *mpt.MPTProof) {
//	fmt.Printf("key=%s , value=%s\n", key, value)
//	mptProof.PrintMPTProof()
//}

// PrintStorageEngine 打印StorageEngine
func (se *StorageEngine) PrintStorageEngine(db *leveldb.DB) {
	if se == nil {
		return
	}
	fmt.Println("打印StorageEngine-------------------------------------------------------------------------------------------")
	fmt.Printf("seHash:%x\n", se.seHash)
	fmt.Printf("primaryIndexHash:%x\n", se.primaryIndexHash)
	se.GetPrimaryIndex(db).PrintMPT(db)
	fmt.Printf("se.secondaryIndexMode:%s\n", se.secondaryIndexMode)
	if se.secondaryIndexMode == "mpt" {
		fmt.Printf("secondaryIndexHash(mpt):%x\n", se.secondaryIndexHashMpt)
		se.GetSecondaryIndexMpt(db).PrintMPT(db)
	} else if se.secondaryIndexMode == "meht" {
		fmt.Printf("secondaryIndexRootHash(meht):%s\n", se.secondaryIndexHashMeht)
		se.GetSecondaryIndexMeht(db).PrintMEHT(db, false)
	} else if se.secondaryIndexMode == "mbt" {
		fmt.Printf("secondaryIndexRootHash(mbt):%s\n", se.secondaryIndexHashMbt)
		se.GetSecondaryIndexMbt(db).PrintMBT(db)
	}
}

// SeStorageEngine 用于序列化StorageEngine的结构体
type SeStorageEngine struct {
	SeHash               [32]byte //搜索引擎的哈希值，由主索引根哈希和辅助索引根哈希计算得到
	PrimaryIndexRootHash [32]byte // 主键索引的根哈希值

	SecondaryIndexMode     string   // 标识当前采用的非主键索引的类型，mpt或meht
	SecondaryIndexHashMpt  [32]byte //mpt类型的非主键索引根哈希
	SecondaryIndexHashMbt  [32]byte
	SecondaryIndexHashMeht [32]byte

	MBTArgs  []interface{}
	MEHTArgs []interface{}
}

// SerializeStorageEngine 序列化存储引擎
func SerializeStorageEngine(se *StorageEngine) []byte {
	//fmt.Println("seSe: ", se.secondaryIndexHashMeht)
	seSe := &SeStorageEngine{se.seHash, se.primaryIndexHash,
		se.secondaryIndexMode, se.secondaryIndexHashMpt,
		se.secondaryIndexHashMbt, se.secondaryIndexHashMeht,
		se.mbtArgs, se.mehtArgs}
	jsonSE, err := json.Marshal(seSe)
	if err != nil {
		fmt.Printf("SerializeStorageEngine error: %v\n", err)
		return nil
	}
	return jsonSE
}

// DeserializeStorageEngine 反序列化存储引擎
func DeserializeStorageEngine(seString []byte, cacheEnable bool, cacheCapacity []interface{}) (*StorageEngine, error) {
	var seSe SeStorageEngine
	err := json.Unmarshal(seString, &seSe)
	if err != nil {
		fmt.Printf("DeserializeStorageEngine error: %v\n", err)
		return nil, err
	}
	fmt.Println("DeserializeStorageEngine:", seSe.MEHTArgs)
	se := &StorageEngine{seSe.SeHash, nil, seSe.PrimaryIndexRootHash, seSe.SecondaryIndexMode,
		nil, seSe.SecondaryIndexHashMpt, nil,
		seSe.SecondaryIndexHashMbt, nil,
		seSe.SecondaryIndexHashMeht, seSe.MBTArgs, seSe.MEHTArgs, cacheEnable, cacheCapacity, sync.RWMutex{}, sync.RWMutex{},
		sync.Mutex{}, sync.Mutex{}}
	return se, nil
}
