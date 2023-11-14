package sedb

import (
	"MEHT/meht"
	"MEHT/mpt"
	"MEHT/util"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"

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

	secondaryIndexMode string // 标识当前采用的非主键索引的类型，mpt或meht

	secondaryIndex_mpt     *mpt.MPT // mpt类型的非主键索引
	secondaryIndexHash_mpt []byte   //mpt类型的非主键索引根哈希

	secondaryIndex_meht *meht.MEHT // meht类型的非主键索引，在db中用mehtName+"meht"索引

	mehtName      string //meht的参数，meht的名字，用于区分不同的meht
	rdx           int    //meht的参数，meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	bc            int    //meht的参数，meht中bucket的容量，即每个bucket中最多存储的KVPair数
	bs            int    //meht的参数，meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
	cacheEnable   bool
	cacheCapacity []interface{}
}

// NewStorageEngine() *StorageEngine: 返回一个新的StorageEngine
func NewStorageEngine(siMode string, mehtName string, rdx int, bc int, bs int,
	cacheEnable bool, cacheCapacity []interface{}) *StorageEngine {
	return &StorageEngine{nil, nil, nil, siMode, nil,
		nil, nil, mehtName, rdx, bc, bs,
		cacheEnable, cacheCapacity}
}

// 更新存储引擎的哈希值，并将更新后的存储引擎写入db中
func (se *StorageEngine) UpdateStorageEngineToDB(db *leveldb.DB) []byte {
	//删除db中原有的se
	//if err := db.Delete(se.seHash, nil); err != nil {
	//	fmt.Println("Delete StorageEngine from DB error:", err)
	//}
	//更新seHash的哈希值
	var seHashs []byte
	seHashs = append(seHashs, se.primaryIndexHash...)
	if se.secondaryIndexMode == "mpt" {
		seHashs = append(seHashs, se.secondaryIndexHash_mpt...)
	} else if se.secondaryIndexMode == "meht" {
		seHashs = append(seHashs, se.mehtName+"meht"...)
	} else {
		fmt.Printf("非主键索引类型siMode设置错误\n")
	}
	hash := sha256.Sum256(seHashs)
	se.seHash = hash[:]
	//将更新后的se写入db中
	//if err := db.Put(se.seHash, SerializeStorageEngine(se), nil); err != nil {
	//	fmt.Println("Insert StorageEngine to DB error:", err)
	//}
	return se.seHash
}

// 返回主索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetPrimaryIndex(db *leveldb.DB) *mpt.MPT {
	//如果当前primaryIndex为空，则从数据库中查询
	if se.primaryIndex == nil && len(se.primaryIndexHash) != 0 {
		primaryIndexString, error_ := db.Get(se.primaryIndexHash, nil)
		if error_ == nil {
			primaryIndex, _ := mpt.DeserializeMPT(primaryIndexString)
			se.primaryIndex = primaryIndex
		} else {
			fmt.Printf("GetPrimaryIndex error:%v\n", error_)
		}
	}
	return se.primaryIndex
}

// 返回mpt类型的辅助索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetSecondaryIndex_mpt(db *leveldb.DB) *mpt.MPT {
	//如果当前secondaryIndex_mpt为空，则从数据库中查询
	if se.secondaryIndex_mpt == nil && len(se.secondaryIndexHash_mpt) != 0 {
		secondaryIndexString, _ := db.Get(se.secondaryIndexHash_mpt, nil)
		if len(secondaryIndexString) != 0 {
			secondaryIndex, _ := mpt.DeserializeMPT(secondaryIndexString)
			se.secondaryIndex_mpt = secondaryIndex
		}
	}
	return se.secondaryIndex_mpt
}

// 返回meht类型的辅助索引指针，如果内存中不在，则从数据库中查询，都不在则返回nil
func (se *StorageEngine) GetSecondaryIndex_meht(db *leveldb.DB) *meht.MEHT {
	//如果当前secondaryIndex_meht为空，则从数据库中查询
	if se.secondaryIndex_meht == nil {
		secondaryIndexString, _ := db.Get([]byte(se.mehtName+"meht"), nil)
		if len(secondaryIndexString) != 0 {
			mgtNodeCC, bucketCC, segmentCC, merkleTreeCC := GetCapacity(&se.cacheCapacity)
			se.secondaryIndex_meht, _ = meht.DeserializeMEHT(secondaryIndexString, db, se.cacheEnable, mgtNodeCC,
				bucketCC, segmentCC, merkleTreeCC)
		}
	}
	return se.secondaryIndex_meht
}

// 向StorageEngine中插入一条记录,返回插入后新的seHash，以及插入的证明
func (se *StorageEngine) Insert(kvpair *util.KVPair, db *leveldb.DB) (*mpt.MPTProof, *mpt.MPTProof, *meht.MEHTProof) {
	//插入主键索引
	//如果是第一次插入
	if se.GetPrimaryIndex(db) == nil {
		//创建一个新的主键索引
		se.primaryIndex = mpt.NewMPT()
	}
	//如果主索引中已存在此key，则获取原来的value，并在非主键索引中删除该value-key对
	oldvalue, oldprimaryProof := se.GetPrimaryIndex(db).QueryByKey(kvpair.GetKey(), db)
	if oldvalue == kvpair.GetValue() {
		//fmt.Printf("key=%x , value=%x已存在\n", []byte(kvpair.GetKey()), []byte(kvpair.GetValue()))
		return oldprimaryProof, nil, nil
	}
	//将KV插入到主键索引中
	piRootHash := se.GetPrimaryIndex(db).Insert(kvpair, db)
	piHash := sha256.Sum256(piRootHash)
	se.primaryIndexHash = piHash[:]
	_, primaryProof := se.GetPrimaryIndex(db).QueryByKey(kvpair.GetKey(), db)
	//fmt.Printf("key=%x , value=%x已插入主键索引MPT\n", []byte(kvpair.GetKey()), []byte(kvpair.GetValue()))
	//构造倒排KV
	reversedKV := util.ReverseKVPair(kvpair)
	//插入非主键索引
	if se.secondaryIndexMode == "mpt" {
		//如果oldvalue不为空，则在非主键索引中删除该value-key对

		//插入到mpt类型的非主键索引中
		_, mptProof := se.InsertIntoMPT(reversedKV, db)
		//打印插入结果
		//fmt.Printf("key=%x , value=%x已插入非主键索引MPT\n", []byte(reversedKV.GetKey()), []byte(newValues))
		//mptProof.PrintMPTProof()
		//更新搜索引擎的哈希值
		//se.UpdateStorageEngineToDB(db)
		return primaryProof, mptProof, nil
	} else if se.secondaryIndexMode == "meht" {
		_, mehtProof := se.InsertIntoMEHT(reversedKV, db)
		//打印插入结果
		//fmt.Printf("key=%x , value=%x已插入非主键索引MEHT\n", []byte(reversedKV.GetKey()), []byte(newValues))
		//meht.PrintMEHTProof(mehtProof)
		//更新搜索引擎的哈希值
		//se.UpdateStorageEngineToDB(db)
		return primaryProof, nil, mehtProof
	} else {
		fmt.Printf("非主键索引类型siMode设置错误\n")
		return nil, nil, nil
	}
}

// 插入非主键索引
func (se *StorageEngine) InsertIntoMPT(kvpair *util.KVPair, db *leveldb.DB) (string, *mpt.MPTProof) {
	//如果是第一次插入
	if se.GetSecondaryIndex_mpt(db) == nil {
		//创建一个新的非主键索引
		se.secondaryIndex_mpt = mpt.NewMPT()
	}
	//先查询得到原有value与待插入value合并
	values, mptProof := se.secondaryIndex_mpt.QueryByKey(kvpair.GetKey(), db)
	//用原有values构建待插入的kvpair
	insertedKV := util.NewKVPair(kvpair.GetKey(), values)
	//将新的value插入到kvpair中
	isChange := insertedKV.AddValue(kvpair.GetValue())
	//如果原有values中没有此value，则插入到mpt中
	if isChange {
		seRootHash := se.secondaryIndex_mpt.Insert(insertedKV, db)
		seHash := sha256.Sum256(seRootHash)
		se.secondaryIndexHash_mpt = seHash[:]
		newValues, newProof := se.secondaryIndex_mpt.QueryByKey(insertedKV.GetKey(), db)
		return newValues, newProof
	}
	return values, mptProof
}

// 插入非主键索引
func (se *StorageEngine) InsertIntoMEHT(kvpair *util.KVPair, db *leveldb.DB) (string, *meht.MEHTProof) {
	//如果是第一次插入
	if se.GetSecondaryIndex_meht(db) == nil {
		//创建一个新的非主键索引
		mgtNodeCC := DefaultNodeCacheCapacity
		bucketCC := DefaultBucketCacheCapacity
		segmentCC := DefaultSegmentCacheCapacity
		merkleTreeCC := DefaultMerkleTreeCapacity
		if se.cacheEnable {
			for _, cacheArg := range se.cacheCapacity {
				switch cacheArg.(type) {
				case MgtNodeCacheCapacity:
					mgtNodeCC = cacheArg.(MgtNodeCacheCapacity)
				case BucketCacheCapacity:
					bucketCC = cacheArg.(BucketCacheCapacity)
				case SegmentCacheCapacity:
					segmentCC = cacheArg.(SegmentCacheCapacity)
				case MerkleTreeCacheCapacity:
					merkleTreeCC = cacheArg.(MerkleTreeCacheCapacity)
				default:
					panic("Unknown type " + reflect.TypeOf(cacheArg).String() + " in function InsertIntoMEHT.")
				}
			}
		}
		se.secondaryIndex_meht = meht.NewMEHT(se.mehtName, se.rdx, se.bc, se.bs, db, int(mgtNodeCC), int(bucketCC), int(segmentCC), int(merkleTreeCC), se.cacheEnable)
	}
	//先查询得到原有value与待插入value合并
	values, bucket, segkey, isSegExist, index := se.secondaryIndex_meht.QueryValueByKey(kvpair.GetKey(), db)
	//用原有values构建待插入的kvpair
	insertedKV := util.NewKVPair(kvpair.GetKey(), values)
	//将新的value插入到kvpair中
	isChange := insertedKV.AddValue(kvpair.GetValue())
	//如果原有values中没有此value，则插入到meht中
	if isChange {
		// 这里逻辑也需要转变，因为并发插入的时候可能很多键都相同但被阻塞了一直没写进去，那更新就会有非常多初始值的重复
		// 因此这里不先进行与初始值的合并，而是在后续委托插入的时候进行重复键的值合并，然后一并插入到桶里的时候利用map结构再对插入值与初始值进行合并去重
		_, newValues, newProof := se.secondaryIndex_meht.Insert(insertedKV, db)
		//_, newValues, newProof := se.secondaryIndex_meht.Insert(kvpair, db)
		//更新meht到db
		se.secondaryIndex_meht.UpdateMEHTToDB(db)
		return newValues, newProof
	}
	return values, se.secondaryIndex_meht.GetQueryProof(bucket, segkey, isSegExist, index, db)
}

func GetCapacity(cacheCapacity *[]interface{}) (mgtNodeCC int, bucketCC int, segmentCC int, merkleTreeCC int) {
	mgtNodeCC = int(DefaultNodeCacheCapacity)
	bucketCC = int(DefaultBucketCacheCapacity)
	segmentCC = int(DefaultSegmentCacheCapacity)
	merkleTreeCC = int(DefaultMerkleTreeCapacity)
	for _, capacity := range *cacheCapacity {
		switch capacity.(type) {
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
		fmt.Printf("secondaryIndexRootHash(meht):%s\n", se.mehtName)
		se.GetSecondaryIndex_meht(db).PrintMEHT(db)
	}
}

// 用于序列化StorageEngine的结构体
type SeStorageEngine struct {
	SeHash               []byte //搜索引擎的哈希值，由主索引根哈希和辅助索引根哈希计算得到
	PrimaryIndexRootHash []byte // 主键索引的根哈希值

	SecondaryIndexMode         string // 标识当前采用的非主键索引的类型，mpt或meht
	SecondaryIndexRootHash_mpt []byte //mpt类型的非主键索引根哈希

	MEHTName string //meht的参数，meht的名字，用于区分不同的meht
	Rdx      int    //meht的参数，meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	Bc       int    //meht的参数，meht中bucket的容量，即每个bucket中最多存储的KVPair数
	Bs       int    //meht的参数，meht中bucket中标识segment的位数，1位则可以标识0和1两个s	//
	//MgtNodeCC    MgtNodeCacheCapacity    // meht的参数，meht中mgtNodeCache的容量
	//BucketCC     BucketCacheCapacity     // meht的参数，meht中bucketCache的容量
	//SegmentCC    SegmentCacheCapacity    // meht的参数，meht中segmentCache的容量
	//MerkleTreeCC MerkleTreeCacheCapacity // meht的参数，meht中merkleTreeCache的容量egment

}

// 序列化存储引擎
func SerializeStorageEngine(se *StorageEngine) []byte {
	sese := &SeStorageEngine{se.seHash, se.primaryIndexHash,
		se.secondaryIndexMode, se.secondaryIndexHash_mpt,
		se.mehtName, se.rdx, se.bc, se.bs}
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
		nil, sese.SecondaryIndexRootHash_mpt, nil, sese.MEHTName,
		sese.Rdx, sese.Bc, sese.Bs, cacheEnable, cacheCapacity}
	return se, nil
}

// //说明：每一个读进来的kv对都是一个KVPair，包含key和value，key和value都是string类型。
// //需要先将key和value转化为十六进制，再插入StorageEngine中。在StorageEngine内部，key和value都是[]byte类型。
// //对于查询得到的结果，直接将key由十六进制转化为字符串，value需要split后，再由十六进制转化为字符串。

// func main() {
// 	//测试StorageEngine

// 	//参数设置
// 	// siMode := "meht" //辅助索引类型，meht或mpt
// 	siMode := "mpt"
// 	rdx := 16 //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
// 	bc := 4   //meht中bucket的容量，即每个bucket中最多存储的KVPair数
// 	bs := 1   //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment

// 	//创建一个StorageEngine
// 	storageEngine := db.NewStorageEngine(siMode, rdx, bc, bs)

// 	//读文件创建一个KVPair数组
// 	kvPairs := util.ReadKVPairFromFile("/home/jaling/Storage/index/meht/data/testdata.txt")

// 	//插入KVPair数组
// 	for i := 0; i < len(kvPairs); i++ {
// 		//把KV转化为十六进制
// 		kvPairs[i].SetKey(util.StringToHex(kvPairs[i].GetKey()))
// 		kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
// 		//插入StorageEngine
// 		storageEngine.Insert(kvPairs[i])
// 	}

// 	// fmt.Printf("len(kvPairs)=%d\n", len(kvPairs))

// 	// //打印主索引
// 	// fmt.Printf("打印主索引-------------------------------------------------------------------------------------------\n")
// 	// storageEngine.GetPrimaryIndex().PrintMPT()
// 	// //打印辅助索引
// 	// fmt.Printf("打印辅助索引-------------------------------------------------------------------------------------------\n")
// 	// storageEngine.GetSecondaryIndex_mpt().PrintMPT()
// 	// storageEngine.GetSecondaryIndex_meht().PrintMEHT()

// 	// //测试查询（MEHT）
// 	// fmt.Printf("测试查询-------------------------------------------------------------------------------------------\n")
// 	// qk_meht := "Bob"
// 	// qv_meht, qvProof_meht := storageEngine.GetSecondaryIndex_meht().QueryByKey(util.StringToHex(qk_meht)) //需要将qk先转换为十六进制
// 	// qvs_meht := strings.Split(qv_meht, ",")                                                               //将查询结果qv按逗号分隔
// 	// fmt.Printf("key=%s查询结果：\n", qk_meht)
// 	// for i := 0; i < len(qvs_meht); i++ {
// 	// 	fmt.Printf("value=%s\n", util.HexToString(qvs_meht[i])) //将分裂后的查询结果转换为字符串输出
// 	// }
// 	// //打印查询结果（MEHT）
// 	// meht.PrintQueryResult(qk_meht, qv_meht, qvProof_meht)
// 	// //验证查询结果（MEHT）
// 	// // storageEngine.GetSecondaryIndex_mpt().VerifyQueryResult(qvBob, qvBobProof)
// 	// meht.VerifyQueryResult(qv_meht, qvProof_meht)

// 	//测试查询（MPT）
// 	fmt.Printf("测试查询-------------------------------------------------------------------------------------------\n")
// 	qk_mpt := "Alice"
// 	qv_mpt, qvProof_mpt := storageEngine.GetSecondaryIndex_mpt().QueryByKey(util.StringToHex(qk_mpt)) //需要将qk先转换为十六进制
// 	qvs_mpt := strings.Split(qv_mpt, ",")                                                             //将查询结果qv按逗号分隔
// 	fmt.Printf("key=%s查询结果：\n", qk_mpt)
// 	for i := 0; i < len(qvs_mpt); i++ {
// 		fmt.Printf("value=%s\n", util.HexToString(qvs_mpt[i])) //将分裂后的查询结果转换为字符串输出
// 	}
// 	//打印查询结果（MPT）
// 	storageEngine.GetSecondaryIndex_mpt().PrintQueryResult(qk_mpt, qv_mpt, qvProof_mpt)
// 	//验证查询结果（MPT）
// 	storageEngine.GetSecondaryIndex_mpt().VerifyQueryResult(qv_mpt, qvProof_mpt)

// }
