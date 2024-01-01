package sedb

import (
	"MEHT/mbt"
	"MEHT/meht"
	"MEHT/mpt"
	"MEHT/util"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

//func NewStorageEngine(siMode string, rdx int, bc int, bs int) *StorageEngine {}： 返回一个新的StorageEngine
//func (se *StorageEngine) GetPrimaryIndex(db *leveldb.DB) *mpt.MPT {}： 返回主索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
//func (se *StorageEngine) GetSecondaryIndex_mpt(db *leveldb.DB) *mpt.MPT {}： 返回mpt类型的辅助索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
//func (se *StorageEngine) Insert(kvPair *util.KVPair, db *leveldb.DB) ([]byte, *mpt.MPTProof, *mpt.MPTProof, *meht.MEHTProof) {}： 向StorageEngine中插入一条记录,返回插入后新的seHash，以及插入的证明
//func PrintQueryResult(key string, value string, mptProof *mpt.MPTProof) {}： 打印查询结果
//func (se *StorageEngine) UpdateStorageEngineToDB(db *leveldb.DB) []byte {}： 更新存储引擎的哈希值，并将更新后的存储引擎写入db中
//func (se *StorageEngine) PrintStorageEngine(db *leveldb.DB) {}： 打印StorageEngine
//func SerializeStorageEngine(se *StorageEngine) []byte {}：序列化存储引擎
//func DeserializeStorageEngine(seString []byte) (*StorageEngine, error) {}： 反序列化存储引擎

type StorageEngine struct {
	seHash []byte //搜索引擎的哈希值，由主索引根哈希和辅助索引根哈希计算得到

	primaryIndex     *mpt.MPT // mpt类型的主键索引
	primaryIndexHash []byte   // 主键索引的哈希值，由主键索引根哈希计算得到

	secondaryIndexMode string // 标识当前采用的非主键索引的类型，mpt或meht或mbt

	secondaryIndexMpt      *mpt.MPT // mpt类型的非主键索引
	secondaryIndexHashMpt  []byte   //mpt类型的非主键索引根哈希
	secondaryIndexMbt      *mbt.MBT
	secondaryIndexHashMbt  []byte
	secondaryIndexMeht     *meht.MEHT // meht类型的非主键索引，在db中用mehtName+"meht"索引
	secondaryIndexHashMeht []byte

	mbtArgs  []interface{}
	mehtArgs []interface{}

	cacheEnable          bool
	cacheCapacity        []interface{}
	primaryLatch         sync.RWMutex
	secondaryLatch       sync.RWMutex
	updatePrimaryLatch   sync.Mutex
	updateSecondaryLatch sync.Mutex
}

// NewStorageEngine 返回一个新的StorageEngine
func NewStorageEngine(siMode string, mbtArgs []interface{}, mehtArgs []interface{},
	cacheEnable bool, cacheCapacity []interface{}) *StorageEngine {
	return &StorageEngine{nil, nil, nil, siMode, nil,
		nil, nil, nil, nil, nil, mbtArgs, mehtArgs,
		cacheEnable, cacheCapacity, sync.RWMutex{}, sync.RWMutex{},
		sync.Mutex{}, sync.Mutex{}}
}

// UpdateStorageEngineToDB 更新存储引擎的哈希值，并将更新后的存储引擎写入db中
func (se *StorageEngine) UpdateStorageEngineToDB() {
	//删除db中原有的se
	//if err := db.Delete(se.seHash, nil); err != nil {
	//	fmt.Println("Delete StorageEngine from DB error:", err)
	//}
	//更新seHash的哈希值
	seHashes := make([]byte, 0)
	se.updatePrimaryLatch.Lock()
	se.updateSecondaryLatch.Lock() // 保证se的哈希与辅助索引根哈希是相关联的
	seHashes = append(seHashes, se.primaryIndexHash...)
	if se.secondaryIndexMode == "mpt" {
		seHashes = append(seHashes, se.secondaryIndexHashMpt...)
	} else if se.secondaryIndexMode == "meht" {
		seHashes = append(seHashes, se.secondaryIndexHashMeht...)
	} else if se.secondaryIndexMode == "mbt" {
		seHashes = append(seHashes, se.secondaryIndexHashMbt...)
	} else {
		fmt.Printf("非主键索引类型siMode设置错误\n")
	}
	hash := sha256.Sum256(seHashes)
	se.seHash = hash[:]
	se.updateSecondaryLatch.Unlock()
	se.updatePrimaryLatch.Unlock()
	//将更新后的se写入db中
	//if err := db.Put(se.seHash, SerializeStorageEngine(se), nil); err != nil {
	//	fmt.Println("Insert StorageEngine to DB error:", err)
	//}
}

// GetPrimaryIndex 返回主索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetPrimaryIndex(db *leveldb.DB) *mpt.MPT {
	//如果当前primaryIndex为空，则从数据库中查询
	if se.primaryIndex == nil && len(se.primaryIndexHash) != 0 && se.primaryLatch.TryLock() { // 只允许一个线程重构主索引
		if se.primaryIndex != nil {
			se.primaryLatch.Unlock()
			return se.primaryIndex
		}
		if primaryIndexString, error_ := db.Get(se.primaryIndexHash, nil); error_ == nil {
			shortNodeCC, fullNodeCC, _, _, _, _, _ := GetCapacity(&se.cacheCapacity)
			primaryIndex, _ := mpt.DeserializeMPT(primaryIndexString, db, se.cacheEnable, shortNodeCC, fullNodeCC)
			se.primaryIndex = primaryIndex
		} else {
			fmt.Printf("GetPrimaryIndex error:%v\n", error_)
		}
		se.primaryLatch.Unlock()
	}
	for se.primaryIndex == nil && len(se.primaryIndexHash) != 0 {
	} // 其余线程等待主索引重构
	return se.primaryIndex
}

// GetSecondaryIndexMpt 返回mpt类型的辅助索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetSecondaryIndexMpt(db *leveldb.DB) *mpt.MPT {
	//如果当前secondaryIndex_mpt为空，则从数据库中查询
	if se.secondaryIndexMpt == nil && len(se.secondaryIndexHashMpt) != 0 && se.secondaryLatch.TryLock() {
		if se.secondaryIndexMpt != nil {
			se.secondaryLatch.Unlock()
			return se.secondaryIndexMpt
		}
		if secondaryIndexString, _ := db.Get(se.secondaryIndexHashMpt, nil); len(secondaryIndexString) != 0 {
			shortNodeCC, fullNodeCC, _, _, _, _, _ := GetCapacity(&se.cacheCapacity)
			secondaryIndex, _ := mpt.DeserializeMPT(secondaryIndexString, db, se.cacheEnable, shortNodeCC, fullNodeCC)
			se.secondaryIndexMpt = secondaryIndex
		}
		se.secondaryLatch.Unlock()
	}
	for se.secondaryIndexMpt == nil && len(se.secondaryIndexHashMpt) != 0 {
	}
	return se.secondaryIndexMpt
}

// GetSecondaryIndexMbt 返回mbt类型的辅助索引指针，如果内存中不存在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetSecondaryIndexMbt(db *leveldb.DB) *mbt.MBT {
	//如果当前secondaryIndex_mbt为空，则从数据库中查询
	if se.secondaryIndexMbt == nil && len(se.secondaryIndexHashMbt) != 0 && se.secondaryLatch.TryLock() {
		if se.secondaryIndexMbt != nil {
			se.secondaryLatch.Unlock()
			return se.secondaryIndexMbt
		}
		if secondaryIndexString, _ := db.Get(se.secondaryIndexHashMbt, nil); len(secondaryIndexString) != 0 {
			_, _, mbtNodeCC, _, _, _, _ := GetCapacity(&se.cacheCapacity)
			secondaryIndex, _ := mbt.DeserializeMBT(secondaryIndexString, db, se.cacheEnable, mbtNodeCC)
			se.secondaryIndexMbt = secondaryIndex
		}
		se.secondaryLatch.Unlock()
	}
	for se.secondaryIndexMbt == nil && len(se.secondaryIndexHashMbt) != 0 {
	}
	return se.secondaryIndexMbt
}

// GetSecondaryIndexMeht 返回meht类型的辅助索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetSecondaryIndexMeht(db *leveldb.DB) *meht.MEHT {
	//如果当前secondaryIndex_meht为空，则从数据库中查询，如果数据库中也找不到，则在纯查询空meht场景下会有大问题
	if se.secondaryIndexMeht == nil && len(se.secondaryIndexHashMeht) != 0 && se.secondaryLatch.TryLock() {
		if se.secondaryIndexMeht != nil {
			se.secondaryLatch.Unlock()
			return se.secondaryIndexMeht
		}
		if secondaryIndexString, _ := db.Get(se.secondaryIndexHashMeht, nil); len(secondaryIndexString) != 0 {
			_, _, _, mgtNodeCC, bucketCC, segmentCC, merkleTreeCC := GetCapacity(&se.cacheCapacity)
			se.secondaryIndexMeht, _ = meht.DeserializeMEHT(secondaryIndexString, db, se.cacheEnable, mgtNodeCC,
				bucketCC, segmentCC, merkleTreeCC)
			se.secondaryIndexMeht.GetMGT(db) // 保证从磁盘读取MEHT成功后MGT也被同步从磁盘中读取出
		}
		se.secondaryLatch.Unlock()
	}
	for se.secondaryIndexMeht == nil && len(se.secondaryIndexHashMeht) != 0 {
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
		shortNodeCC, fullNodeCC, _, _, _, _, _ := GetCapacity(&se.cacheCapacity)
		se.primaryIndex = mpt.NewMPT(primaryDb, se.cacheEnable, shortNodeCC, fullNodeCC)
		se.primaryLatch.Unlock()
	}
	for se.primaryIndex == nil {
	} // 其余线程等待主索引新建成功
	//如果主索引中已存在此key，则获取原来的value，并在非主键索引中删除该value-key对
	oldValue, oldPrimaryProof := se.GetPrimaryIndex(primaryDb).QueryByKey(kvPair.GetKey(), primaryDb)
	if oldValue == kvPair.GetValue() {
		//fmt.Printf("key=%x , value=%x已存在\n", []byte(kvPair.GetKey()), []byte(kvPair.GetValue()))
		return oldPrimaryProof, nil, nil
	}
	runtime.GOMAXPROCS(1)
	var wG sync.WaitGroup
	wG.Add(2)
	oldValueCh := make(chan string)
	needDeleteCh := make(chan bool)
	go func() {
		defer wG.Done()
		//将KV插入到主键索引中
		oldVal, needDelete := se.primaryIndex.Insert(kvPair, primaryDb, nil, isUpdate)
		oldValueCh <- oldVal
		needDeleteCh <- needDelete
		se.updatePrimaryLatch.Lock() // 保证se留存的主索引哈希与实际主索引根哈希一致
		se.primaryIndex.GetUpdateLatch().Lock()
		piHash := sha256.Sum256(se.primaryIndex.GetRootHash())
		se.primaryIndexHash = piHash[:]
		se.primaryIndex.GetUpdateLatch().Unlock()
		se.updatePrimaryLatch.Unlock()
		//_, primaryProof := se.GetPrimaryIndex(db).QueryByKey(kvPair.GetKey(), db)
		//fmt.Printf("key=%x , value=%x已插入主键索引MPT\n", []byte(kvPair.GetKey()), []byte(kvPair.GetValue()))
	}()
	//构造倒排KV
	reversedKV := util.ReverseKVPair(kvPair)
	//插入非主键索引
	if se.secondaryIndexMode == "mpt" { //插入mpt
		//如果oldValue不为空，则在非主键索引中删除该value-key对

		//插入到mpt类型的非主键索引中
		//_, mptProof := se.InsertIntoMPT(reversedKV, db)
		go func() { // 实际应该为返回值mptProof构建一个chan并等待输出
			defer wG.Done()
			se.InsertIntoMPT(reversedKV, secondaryDb, se.primaryIndex, false)
			oldVal := <-oldValueCh
			needDelete := <-needDeleteCh
			if needDelete {
				se.InsertIntoMPT(*util.NewKVPair(oldVal, reversedKV.GetValue()), secondaryDb, se.primaryIndex, true)
			}
		}()
		//打印插入结果
		//fmt.Printf("key=%x , value=%x已插入非主键索引MPT\n", []byte(reversedKV.GetKey()), []byte(newValues))
		//mptProof.PrintMPTProof()
		//更新搜索引擎的哈希值
		//se.UpdateStorageEngineToDB(db)
		//return primaryProof, mptProof, nil
	} else if se.secondaryIndexMode == "meht" { //插入meht
		//var mehtProof *meht.MEHTProof
		//_, mehtProof  = se.InsertIntoMEHT(reversedKV, db)
		go func() { //实际应该为返回值mehtProof构建一个chan并等待输出
			defer wG.Done()
			se.InsertIntoMEHT(reversedKV, secondaryDb)
		}()
		//打印插入结果
		//fmt.Printf("key=%x , value=%x已插入非主键索引MEHT\n", []byte(reversedKV.GetKey()), []byte(newValues))
		//meht.PrintMEHTProof(mehtProof)
		//更新搜索引擎的哈希值
		//se.UpdateStorageEngineToDB(db)
		//return primaryProof, nil, mehtProof
	} else {
		go func() {
			defer wG.Done()
			se.InsertIntoMBT(reversedKV, secondaryDb)
			//se.In
		}()
	}
	wG.Wait()
	return nil, nil, nil
}

func (se *StorageEngine) BatchCommit(secondaryDb *leveldb.DB) {
	if se.secondaryIndexMode != "meht" && se.secondaryIndexMode != "mpt" && se.secondaryIndexMode != "mbt" {
		fmt.Printf("非主键索引类型siMode设置错误\n")
		return
	}
	if se.primaryIndex == nil { // 没有主索引说明没有交易需要批量提交
		return
	}
	if se.secondaryIndexMode == "mpt" { //插入mpt
		se.MPTBatchCommit(secondaryDb)
	} else if se.secondaryIndexMode == "meht" { //插入meht
		se.MEHTBatchCommit(secondaryDb)
	} else if se.secondaryIndexMode == "mbt" {
		se.MBTBatchCommit(secondaryDb)
	}
	return
}

// MPTBatchCommit 批量提交mpt
func (se *StorageEngine) MPTBatchCommit(db *leveldb.DB) {
	if se.secondaryIndexMpt == nil { // 没有辅助索引说明没有交易需要批量提交
		return
	}
	// 批量更新mgt，正式提交
	secondaryIndex := se.secondaryIndexMpt
	secondaryIndex.MPTBatchFix(db)
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
	se.updateSecondaryLatch.Lock() // 保证se存储的辅助索引根哈希与实际辅助索引的根哈希是一致的
	se.secondaryIndexMpt.GetUpdateLatch().Lock()
	seHash := sha256.Sum256(se.secondaryIndexMpt.GetRootHash())
	se.secondaryIndexHashMpt = seHash[:]
	se.secondaryIndexMpt.GetUpdateLatch().Unlock()
	se.updateSecondaryLatch.Unlock()
	//newValues, newProof := se.secondaryIndex_mpt.QueryByKey(insertedKV.GetKey(), db)
	//return newValues, newProof
	return "", nil
	//return values, mptProof
}

// MBTBatchCommit 批量提交mbt
func (se *StorageEngine) MBTBatchCommit(db *leveldb.DB) {
	if se.secondaryIndexMbt == nil { // 没有辅助索引说明没有交易需要批量提交
		return
	}
	// 批量更新mgt，正式提交
	secondaryIndex := se.secondaryIndexMbt
	secondaryIndex.MBTBatchFix(db)
}

// InsertIntoMBT 插入非主键索引
func (se *StorageEngine) InsertIntoMBT(kvPair util.KVPair, db *leveldb.DB) (string, *mbt.MBTProof) {
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
	path := mbt.ComputePath(se.secondaryIndexMbt.GetBucketNum(), se.secondaryIndexMbt.GetAggregation(), se.secondaryIndexMbt.GetGd(), kvPair.GetKey())
	values, mbtProof := se.secondaryIndexMbt.QueryByKey(kvPair.GetKey(), path, db, false)
	//用原有values插入到kvPair中
	insertedKV := util.NewKVPair(kvPair.GetKey(), values)
	isChange := insertedKV.AddValue(kvPair.GetValue())
	//如果原有values中没有此value，则插入到mpt中
	if isChange {
		se.secondaryIndexMbt.Insert(kvPair, db)
		se.updateSecondaryLatch.Lock() // 保证se存储的辅助索引根哈希与实际辅助索引的根哈希是一致的
		se.secondaryIndexMbt.GetUpdateLatch().Lock()
		se.secondaryIndexHashMbt = se.secondaryIndexMbt.GetMBTHash()
		se.secondaryIndexMbt.GetUpdateLatch().Unlock()
		se.updateSecondaryLatch.Unlock()
		//newValues, newProof := se.secondaryIndex_mpt.QueryByKey(insertedKV.GetKey(), db)
		//return newValues, newProof
		return "", nil
	}
	return values, mbtProof
}

func (se *StorageEngine) MEHTBatchCommit(db *leveldb.DB) {
	if se.secondaryIndexMeht == nil { // 没有辅助索引说明没有交易需要批量提交
		return
	}
	// 批量更新mgt，正式提交
	se.secondaryIndexMeht.MGTBatchCommit(db)
	// 批量更新会更新mgt的rootHash,因此需要更新se的辅助索引根哈希
	se.secondaryIndexMeht.UpdateMEHTToDB(db)
}

// InsertIntoMEHT 插入非主键索引
func (se *StorageEngine) InsertIntoMEHT(kvPair util.KVPair, db *leveldb.DB) (string, *meht.MEHTProof) {
	//如果是第一次插入
	if se.GetSecondaryIndexMeht(db) == nil && se.secondaryLatch.TryLock() { // 总有一个线程会获得锁并创建meht
		//创建一个新的非主键索引
		_, _, _, mgtNodeCC, bucketCC, segmentCC, merkleTreeCC := GetCapacity(&se.cacheCapacity)
		mehtRdx, mehtBc, mehtBs := GetMEHTArgs(&se.mehtArgs)
		se.secondaryIndexMeht = meht.NewMEHT(mehtRdx, mehtBc, mehtBs, db, mgtNodeCC, bucketCC, segmentCC, merkleTreeCC, se.cacheEnable)
		se.secondaryLatch.Unlock()
	}
	for se.GetSecondaryIndexMeht(db) == nil { // 其余线程等待meht创建成功
	}
	//先查询得到原有value与待插入value合并
	values, bucket, segKey, isSegExist, index := se.secondaryIndexMeht.QueryValueByKey(kvPair.GetKey(), db)
	//用原有values构建待插入的kvPair
	insertedKV := util.NewKVPair(kvPair.GetKey(), values)
	//将新的value插入到kvPair中
	isChange := insertedKV.AddValue(kvPair.GetValue())
	//如果原有values中没有此value，则插入到meht中
	if isChange {
		// 这里逻辑也需要转变，因为并发插入的时候可能很多键都相同但被阻塞了一直没写进去，那更新就会有非常多初始值的重复
		// 因此这里不先进行与初始值的合并，而是在后续委托插入的时候进行重复键的值合并，然后一并插入到桶里的时候利用map结构再对插入值与初始值进行合并去重
		//_, newValues, newProof := se.secondaryIndex_meht.Insert(insertedKV, db)
		_, newValues, newProof := se.secondaryIndexMeht.Insert(kvPair, db)
		//本来是需要更新meht到db，但是现在是批量插入，mgtHash是脏的，所以更新没有意义
		//se.secondaryIndexMeht.UpdateMEHTToDB(db)
		return newValues, newProof
	}
	return values, se.secondaryIndexMeht.GetQueryProof(bucket, segKey, isSegExist, index, db)
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
func GetMEHTArgs(mehtArgs *[]interface{}) (mehtRdx int, mehtBc int, mehtBs int) {
	mehtRdx = int(DefaultMEHTRdx)
	mehtBc = int(DefaultMEHTBc)
	mehtBs = int(DefaultMEHTBs)
	for _, arg := range *mehtArgs {
		switch arg.(type) {
		case MEHTRdx:
			mehtRdx = int(arg.(MEHTRdx))
		case MEHTBc:
			mehtBc = int(arg.(MEHTBc))
		case MEHTBs:
			mehtBs = int(arg.(MEHTBs))
		default:
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
		se.GetSecondaryIndexMeht(db).PrintMEHT(db)
	} else if se.secondaryIndexMode == "mbt" {
		fmt.Printf("secondaryIndexRootHash(mbt):%s\n", se.secondaryIndexHashMbt)
		se.GetSecondaryIndexMbt(db).PrintMBT(db)
	}
}

// SeStorageEngine 用于序列化StorageEngine的结构体
type SeStorageEngine struct {
	SeHash               []byte //搜索引擎的哈希值，由主索引根哈希和辅助索引根哈希计算得到
	PrimaryIndexRootHash []byte // 主键索引的根哈希值

	SecondaryIndexMode         string // 标识当前采用的非主键索引的类型，mpt或meht
	SecondaryIndexRootHashMpt  []byte //mpt类型的非主键索引根哈希
	SecondaryIndexRootHashMbt  []byte
	SecondaryIndexRootHashMeht []byte

	MBTArgs  []interface{}
	MEHTArgs []interface{}
}

// SerializeStorageEngine 序列化存储引擎
func SerializeStorageEngine(se *StorageEngine) []byte {
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
	se := &StorageEngine{seSe.SeHash, nil, seSe.PrimaryIndexRootHash, seSe.SecondaryIndexMode,
		nil, seSe.SecondaryIndexRootHashMpt, nil,
		seSe.SecondaryIndexRootHashMbt, nil,
		seSe.SecondaryIndexRootHashMeht, seSe.MBTArgs, seSe.MEHTArgs, cacheEnable, cacheCapacity, sync.RWMutex{}, sync.RWMutex{},
		sync.Mutex{}, sync.Mutex{}}
	return se, nil
}
