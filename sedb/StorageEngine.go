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
//func (se *StorageEngine) Insert(kvpair *util.KVPair, db *leveldb.DB) ([]byte, *mpt.MPTProof, *mpt.MPTProof, *meht.MEHTProof) {}： 向StorageEngine中插入一条记录,返回插入后新的seHash，以及插入的证明
//func PrintQueryResult(key string, value string, mptProof *mpt.MPTProof) {}： 打印查询结果
//func (se *StorageEngine) UpdataStorageEngineToDB(db *leveldb.DB) []byte {}： 更新存储引擎的哈希值，并将更新后的存储引擎写入db中
//func (se *StorageEngine) PrintStorageEngine(db *leveldb.DB) {}： 打印StorageEngine
//func SerializeStorageEngine(se *StorageEngine) []byte {}：序列化存储引擎
//func DeserializeStorageEngine(sestring []byte) (*StorageEngine, error) {}： 反序列化存储引擎

type StorageEngine struct {
	seHash []byte //搜索引擎的哈希值，由主索引根哈希和辅助索引根哈希计算得到

	primaryIndex     *mpt.MPT // mpt类型的主键索引
	primaryIndexHash []byte   // 主键索引的哈希值，由主键索引根哈希计算得到

	secondaryIndexMode string // 标识当前采用的非主键索引的类型，mpt或meht或mbt

	secondaryIndex_mpt      *mpt.MPT // mpt类型的非主键索引
	secondaryIndexHash_mpt  []byte   //mpt类型的非主键索引根哈希
	secondaryIndex_mbt      *mbt.MBT
	secondaryIndexHash_mbt  []byte
	secondaryIndex_meht     *meht.MEHT // meht类型的非主键索引，在db中用mehtName+"meht"索引
	secondaryIndexHash_meht []byte

	mbtArgs  []interface{}
	mehtArgs []interface{}

	cacheEnable          bool
	cacheCapacity        []interface{}
	primaryLatch         sync.RWMutex
	secondaryLatch       sync.RWMutex
	updatePrimaryLatch   sync.Mutex
	updateSecondaryLatch sync.Mutex
}

// NewStorageEngine() *StorageEngine: 返回一个新的StorageEngine
func NewStorageEngine(siMode string, mbtArgs []interface{}, mehtArgs []interface{},
	cacheEnable bool, cacheCapacity []interface{}) *StorageEngine {
	return &StorageEngine{nil, nil, nil, siMode, nil,
		nil, nil, nil, nil, nil, mbtArgs, mehtArgs,
		cacheEnable, cacheCapacity, sync.RWMutex{}, sync.RWMutex{},
		sync.Mutex{}, sync.Mutex{}}
}

// 更新存储引擎的哈希值，并将更新后的存储引擎写入db中
func (se *StorageEngine) UpdateStorageEngineToDB(db *leveldb.DB) {
	//删除db中原有的se
	//if err := db.Delete(se.seHash, nil); err != nil {
	//	fmt.Println("Delete StorageEngine from DB error:", err)
	//}
	//更新seHash的哈希值
	seHashs := make([]byte, 0)
	se.updatePrimaryLatch.Lock()
	se.updateSecondaryLatch.Lock() // 保证se的哈希与辅助索引根哈希是相关联的
	seHashs = append(seHashs, se.primaryIndexHash...)
	if se.secondaryIndexMode == "mpt" {
		seHashs = append(seHashs, se.secondaryIndexHash_mpt...)
	} else if se.secondaryIndexMode == "meht" {
		seHashs = append(seHashs, se.secondaryIndexHash_meht...)
	} else if se.secondaryIndexMode == "mbt" {
		seHashs = append(seHashs, se.secondaryIndexHash_mbt...)
	} else {
		fmt.Printf("非主键索引类型siMode设置错误\n")
	}
	hash := sha256.Sum256(seHashs)
	se.seHash = hash[:]
	se.updateSecondaryLatch.Unlock()
	se.updatePrimaryLatch.Unlock()
	//将更新后的se写入db中
	//if err := db.Put(se.seHash, SerializeStorageEngine(se), nil); err != nil {
	//	fmt.Println("Insert StorageEngine to DB error:", err)
	//}
}

// 返回主索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
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

// 返回mpt类型的辅助索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetSecondaryIndex_mpt(db *leveldb.DB) *mpt.MPT {
	//如果当前secondaryIndex_mpt为空，则从数据库中查询
	if se.secondaryIndex_mpt == nil && len(se.secondaryIndexHash_mpt) != 0 && se.secondaryLatch.TryLock() {
		if se.secondaryIndex_mpt != nil {
			se.secondaryLatch.Unlock()
			return se.secondaryIndex_mpt
		}
		if secondaryIndexString, _ := db.Get(se.secondaryIndexHash_mpt, nil); len(secondaryIndexString) != 0 {
			shortNodeCC, fullNodeCC, _, _, _, _, _ := GetCapacity(&se.cacheCapacity)
			secondaryIndex, _ := mpt.DeserializeMPT(secondaryIndexString, db, se.cacheEnable, shortNodeCC, fullNodeCC)
			se.secondaryIndex_mpt = secondaryIndex
		}
	}
	for se.secondaryIndex_mpt == nil && len(se.secondaryIndexHash_mpt) != 0 {
	}
	return se.secondaryIndex_mpt
}

// 返回mbt类型的辅助索引指针，如果内存中不存在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetSecondaryIndex_mbt(db *leveldb.DB) *mbt.MBT {
	//如果当前secondaryIndex_mbt为空，则从数据库中查询
	if se.secondaryIndex_mbt == nil && len(se.secondaryIndexHash_mbt) != 0 && se.secondaryLatch.TryLock() {
		if se.secondaryIndex_mbt != nil {
			se.secondaryLatch.Unlock()
			return se.secondaryIndex_mbt
		}
		if secondaryIndexString, _ := db.Get(se.secondaryIndexHash_mbt, nil); len(secondaryIndexString) != 0 {
			_, _, mbtNodeCC, _, _, _, _ := GetCapacity(&se.cacheCapacity)
			secondaryIndex, _ := mbt.DeserializeMBT(secondaryIndexString, db, se.cacheEnable, mbtNodeCC)
			se.secondaryIndex_mbt = secondaryIndex
		}
	}
	for se.secondaryIndex_mbt == nil && len(se.secondaryIndexHash_mbt) != 0 {
	}
	return se.secondaryIndex_mbt
}

// 返回meht类型的辅助索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetSecondaryIndex_meht(db *leveldb.DB) *meht.MEHT {
	//如果当前secondaryIndex_meht为空，则从数据库中查询，如果数据库中也找不到，则在纯查询空meht场景下会有大问题
	if se.secondaryIndex_meht == nil && len(se.secondaryIndexHash_meht) != 0 && se.secondaryLatch.TryLock() {
		if se.secondaryIndex_meht != nil {
			se.secondaryLatch.Unlock()
			return se.secondaryIndex_meht
		}
		if secondaryIndexString, _ := db.Get(se.secondaryIndexHash_meht, nil); len(secondaryIndexString) != 0 {
			_, _, _, mgtNodeCC, bucketCC, segmentCC, merkleTreeCC := GetCapacity(&se.cacheCapacity)
			se.secondaryIndex_meht, _ = meht.DeserializeMEHT(secondaryIndexString, db, se.cacheEnable, mgtNodeCC,
				bucketCC, segmentCC, merkleTreeCC)
			se.secondaryIndex_meht.GetMGT(db) // 保证从磁盘读取MEHT成功后MGT也被同步从磁盘中读取出
		}
	}
	for se.secondaryIndex_meht == nil && len(se.secondaryIndexHash_meht) != 0 {
	}
	return se.secondaryIndex_meht
}

// 向StorageEngine中插入一条记录,返回插入后新的seHash，以及插入的证明
func (se *StorageEngine) Insert(kvpair util.KVPair, primaryDb *leveldb.DB, secondaryDb *leveldb.DB) (*mpt.MPTProof, *mpt.MPTProof, *meht.MEHTProof) {
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
	oldvalue, oldprimaryProof := se.GetPrimaryIndex(primaryDb).QueryByKey(kvpair.GetKey(), primaryDb, false)
	if oldvalue == kvpair.GetValue() {
		//fmt.Printf("key=%x , value=%x已存在\n", []byte(kvpair.GetKey()), []byte(kvpair.GetValue()))
		return oldprimaryProof, nil, nil
	}
	runtime.GOMAXPROCS(1)
	var wG sync.WaitGroup
	wG.Add(2)
	go func() {
		defer wG.Done()
		//将KV插入到主键索引中
		se.primaryIndex.Insert(kvpair, primaryDb)
		se.updatePrimaryLatch.Lock() // 保证se留存的主索引哈希与实际主索引根哈希一致
		se.primaryIndex.GetUpdateLatch().Lock()
		piHash := sha256.Sum256(se.primaryIndex.GetRootHash())
		se.primaryIndexHash = piHash[:]
		se.primaryIndex.GetUpdateLatch().Unlock()
		se.updatePrimaryLatch.Unlock()
		//_, primaryProof := se.GetPrimaryIndex(db).QueryByKey(kvpair.GetKey(), db)
		//fmt.Printf("key=%x , value=%x已插入主键索引MPT\n", []byte(kvpair.GetKey()), []byte(kvpair.GetValue()))
	}()
	//构造倒排KV
	reversedKV := util.ReverseKVPair(kvpair)
	//插入非主键索引
	if se.secondaryIndexMode == "mpt" { //插入mpt
		//如果oldvalue不为空，则在非主键索引中删除该value-key对

		//插入到mpt类型的非主键索引中
		//_, mptProof := se.InsertIntoMPT(reversedKV, db)
		go func() { // 实际应该为返回值mptProof构建一个chan并等待输出
			defer wG.Done()
			se.InsertIntoMPT(reversedKV, secondaryDb)
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

// 插入非主键索引
func (se *StorageEngine) InsertIntoMPT(kvPair util.KVPair, db *leveldb.DB) (string, *mpt.MPTProof) {
	//如果是第一次插入
	if se.GetSecondaryIndex_mpt(db) == nil && se.secondaryLatch.TryLock() { // 总有一个线程会拿到写锁并创建非主键索引
		//创建一个新的非主键索引
		shortNodeCC, fullNodeCC, _, _, _, _, _ := GetCapacity(&se.cacheCapacity)
		se.secondaryIndex_mpt = mpt.NewMPT(db, se.cacheEnable, shortNodeCC, fullNodeCC)
		se.secondaryLatch.Unlock()
	}
	for se.secondaryIndex_mpt == nil { // 其余线程等待非主键索引创建
	}
	//先查询得到原有value与待插入value合并
	values, mptProof := se.secondaryIndex_mpt.QueryByKey(kvPair.GetKey(), db, false)
	//将原有values插入到kvPair中
	isChange := kvPair.AddValue(values)
	//如果原有values中没有此value，则插入到mpt中
	if isChange {
		se.secondaryIndex_mpt.Insert(kvPair, db)
		se.updateSecondaryLatch.Lock() // 保证se存储的辅助索引根哈希与实际辅助索引的根哈希是一致的
		se.secondaryIndex_mpt.GetUpdateLatch().Lock()
		seHash := sha256.Sum256(se.secondaryIndex_mpt.GetRootHash())
		se.secondaryIndexHash_mpt = seHash[:]
		se.secondaryIndex_mpt.GetUpdateLatch().Unlock()
		se.updateSecondaryLatch.Unlock()
		//newValues, newProof := se.secondaryIndex_mpt.QueryByKey(insertedKV.GetKey(), db)
		//return newValues, newProof
		return "", nil
	}
	return values, mptProof
}

func (se *StorageEngine) InsertIntoMBT(kvPair util.KVPair, db *leveldb.DB) (string, *mbt.MBTProof) {
	//如果是第一次插入
	if se.GetSecondaryIndex_mbt(db) == nil && se.secondaryLatch.TryLock() { // 总有一个线程会拿到写锁并创建非主键索引
		//创建一个新的非主键索引
		_, _, mbtNodeCC, _, _, _, _ := GetCapacity(&se.cacheCapacity)
		bucketNum, aggregation := GetMBTArgs(&se.mbtArgs)
		se.secondaryIndex_mbt = mbt.NewMBT(bucketNum, aggregation, db, se.cacheEnable, mbtNodeCC)
		se.secondaryLatch.Unlock()
	}
	for se.secondaryIndex_mbt == nil { // 其余线程等待非主键索引创建
	}
	//先查询得到原有value与待插入value合并
	path := mbt.ComputePath(se.secondaryIndex_mbt.GetBucketNum(), se.secondaryIndex_mbt.GetAggregation(), kvPair.GetKey())
	values, mbtProof := se.secondaryIndex_mbt.QueryByKey(kvPair.GetKey(), path, db, false)
	//用原有values插入到kvPair中
	isChange := kvPair.AddValue(values)
	//如果原有values中没有此value，则插入到mpt中
	if isChange {
		se.secondaryIndex_mbt.Insert(kvPair, db)
		se.updateSecondaryLatch.Lock() // 保证se存储的辅助索引根哈希与实际辅助索引的根哈希是一致的
		se.secondaryIndex_mbt.GetUpdateLatch().Lock()
		seHash := sha256.Sum256(se.secondaryIndex_mbt.GetRootHash())
		se.secondaryIndexHash_mbt = seHash[:]
		se.secondaryIndex_mbt.GetUpdateLatch().Unlock()
		se.updateSecondaryLatch.Unlock()
		//newValues, newProof := se.secondaryIndex_mpt.QueryByKey(insertedKV.GetKey(), db)
		//return newValues, newProof
		return "", nil
	}
	return values, mbtProof
}

// 插入非主键索引
func (se *StorageEngine) InsertIntoMEHT(kvPair util.KVPair, db *leveldb.DB) (string, *meht.MEHTProof) {
	//如果是第一次插入
	if se.GetSecondaryIndex_meht(db) == nil && se.secondaryLatch.TryLock() { // 总有一个线程会获得锁并创建meht
		//创建一个新的非主键索引
		_, _, _, mgtNodeCC, bucketCC, segmentCC, merkleTreeCC := GetCapacity(&se.cacheCapacity)
		mehtRdx, mehtBc, mehtBs := GetMEHTArgs(&se.mehtArgs)
		se.secondaryIndex_meht = meht.NewMEHT(mehtRdx, mehtBc, mehtBs, db, mgtNodeCC, bucketCC, segmentCC, merkleTreeCC, se.cacheEnable)
		se.secondaryLatch.Unlock()
	}
	for se.GetSecondaryIndex_meht(db) == nil { // 其余线程等待meht创建成功
	}
	//先查询得到原有value与待插入value合并
	values, bucket, segkey, isSegExist, index := se.secondaryIndex_meht.QueryValueByKey(kvPair.GetKey(), db)
	//用原有values构建待插入的kvpair
	insertedKV := util.NewKVPair(kvPair.GetKey(), values)
	//将新的value插入到kvpair中
	isChange := insertedKV.AddValue(kvPair.GetValue())
	//如果原有values中没有此value，则插入到meht中
	if isChange {
		// 这里逻辑也需要转变，因为并发插入的时候可能很多键都相同但被阻塞了一直没写进去，那更新就会有非常多初始值的重复
		// 因此这里不先进行与初始值的合并，而是在后续委托插入的时候进行重复键的值合并，然后一并插入到桶里的时候利用map结构再对插入值与初始值进行合并去重
		//_, newValues, newProof := se.secondaryIndex_meht.Insert(insertedKV, db)
		_, newValues, newProof := se.secondaryIndex_meht.Insert(kvPair, db)
		//更新meht到db
		se.secondaryIndex_meht.UpdateMEHTToDB(db)
		return newValues, newProof
	}
	return values, se.secondaryIndex_meht.GetQueryProof(bucket, segkey, isSegExist, index, db)
}

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

// 打印查询结果
func PrintQueryResult(key string, value string, mptProof *mpt.MPTProof) {
	fmt.Printf("key=%s , value=%s\n", key, value)
	mptProof.PrintMPTProof()
}

// 打印StorageEngine
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
		fmt.Printf("secondaryIndexHash(mpt):%x\n", se.secondaryIndexHash_mpt)
		se.GetSecondaryIndex_mpt(db).PrintMPT(db)
	} else if se.secondaryIndexMode == "meht" {
		fmt.Printf("secondaryIndexRootHash(meht):%s\n", se.secondaryIndexHash_meht)
		se.GetSecondaryIndex_meht(db).PrintMEHT(db)
	} else if se.secondaryIndexMode == "mbt" {
		fmt.Printf("secondaryIndexRootHash(mbt):%s\n", se.secondaryIndexHash_mbt)
		se.GetSecondaryIndex_mbt(db).PrintMBT(db)
	}
}

// 用于序列化StorageEngine的结构体
type SeStorageEngine struct {
	SeHash               []byte //搜索引擎的哈希值，由主索引根哈希和辅助索引根哈希计算得到
	PrimaryIndexRootHash []byte // 主键索引的根哈希值

	SecondaryIndexMode          string // 标识当前采用的非主键索引的类型，mpt或meht
	SecondaryIndexRootHash_mpt  []byte //mpt类型的非主键索引根哈希
	SecondaryIndexRootHash_mbt  []byte
	SecondaryIndexRootHash_meht []byte

	MBTArgs  []interface{}
	MEHTArgs []interface{}
}

// 序列化存储引擎
func SerializeStorageEngine(se *StorageEngine) []byte {
	sese := &SeStorageEngine{se.seHash, se.primaryIndexHash,
		se.secondaryIndexMode, se.secondaryIndexHash_mpt,
		se.secondaryIndexHash_mbt, se.secondaryIndexHash_meht,
		se.mbtArgs, se.mehtArgs}
	jsonSE, err := json.Marshal(sese)
	if err != nil {
		fmt.Printf("SerializeStorageEngine error: %v\n", err)
		return nil
	}
	return jsonSE
}

// 反序列化存储引擎
func DeserializeStorageEngine(sestring []byte, cacheEnable bool, cacheCapacity []interface{}) (*StorageEngine, error) {
	var sese SeStorageEngine
	err := json.Unmarshal(sestring, &sese)
	if err != nil {
		fmt.Printf("DeserializeStorageEngine error: %v\n", err)
		return nil, err
	}
	se := &StorageEngine{sese.SeHash, nil, sese.PrimaryIndexRootHash, sese.SecondaryIndexMode,
		nil, sese.SecondaryIndexRootHash_mpt, nil,
		sese.SecondaryIndexRootHash_mbt, nil,
		sese.SecondaryIndexRootHash_meht, sese.MBTArgs, sese.MEHTArgs, cacheEnable, cacheCapacity, sync.RWMutex{}, sync.RWMutex{},
		sync.Mutex{}, sync.Mutex{}}
	return se, nil
}
