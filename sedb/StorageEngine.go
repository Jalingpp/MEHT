package sedb

import (
	"MEHT/meht"
	"MEHT/mpt"
	"MEHT/util"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

type StorageEngine struct {
	seHash []byte //搜索引擎的哈希值，由主索引根哈希和辅助索引根哈希计算得到

	primaryIndex         *mpt.MPT // mpt类型的主键索引
	primaryIndexRootHash []byte   // 主键索引的根哈希值

	secondaryIndexMode string // 标识当前采用的非主键索引的类型，mpt或meht

	secondaryIndex_mpt         *mpt.MPT // mpt类型的非主键索引
	secondaryIndexRootHash_mpt []byte   //mpt类型的非主键索引根哈希

	secondaryIndex_meht *meht.MEHT // meht类型的非主键索引
	// secondaryIndexRootHash_meht []byte     //meht类型的非主键索引根哈希
}

// NewStorageEngine() *StorageEngine: 返回一个新的StorageEngine
func NewStorageEngine(seh []byte, prh []byte, siMode string, srh []byte, rdx int, bc int, bs int, db *leveldb.DB) *StorageEngine {
	//如果seh不为空，则从数据库中查询
	//如果seh不为空，则从数据库中查询
	var storengine *StorageEngine
	if seh != nil {
		sehString, _ := db.Get(seh, nil)
		if sehString != nil {
			storengine, _ = DeserializeStorageEngine(sehString)
		} else {
			fmt.Printf("leveldb中不存在以seHash=%x为key的StorageEngine\n", seh)
		}
	} else {
		storengine = &StorageEngine{seh, nil, prh, siMode, nil, srh, nil}
	}
	storengine.primaryIndex = mpt.NewMPT(prh, db)
	storengine.secondaryIndex_mpt = mpt.NewMPT(srh, db)
	storengine.secondaryIndex_meht = meht.NewMEHT(rdx, bc, bs)
	return storengine
}

// GetPrimaryIndex() *mpt.MPT: 返回primaryIndex
func (se *StorageEngine) GetPrimaryIndex() *mpt.MPT {
	return se.primaryIndex
}

// GetSecondaryIndex_mpt() *mpt.MPT: 返回secondaryIndex_mpt
func (se *StorageEngine) GetSecondaryIndex_mpt() *mpt.MPT {
	return se.secondaryIndex_mpt
}

// GetSecondaryIndex_meht() *meht.MEHT: 返回secondaryIndex_meht
func (se *StorageEngine) GetSecondaryIndex_meht() *meht.MEHT {
	return se.secondaryIndex_meht
}

// 向StorageEngine中插入一条记录,返回插入后新的seHash，以及插入的证明
func (se *StorageEngine) Insert(kvpair *util.KVPair, db *leveldb.DB) ([]byte, *mpt.MPTProof, *mpt.MPTProof, *meht.MEHTProof) {
	//插入主键索引
	se.primaryIndex.Insert(kvpair)
	_, primaryProof := se.primaryIndex.QueryByKey(kvpair.GetKey())
	fmt.Printf("key=%x , value=%x已插入主键索引MPT\n", []byte(kvpair.GetKey()), []byte(kvpair.GetValue()))
	//构造倒排KV
	reversedKV := util.ReverseKVPair(kvpair)
	//插入非主键索引
	if se.secondaryIndexMode == "mpt" {
		newValues, mptProof := se.InsertIntoMPT(reversedKV)
		//打印插入结果
		fmt.Printf("key=%x , value=%x已插入非主键索引MPT\n", []byte(reversedKV.GetKey()), []byte(newValues))
		mptProof.PrintMPTProof()
		//更新搜索引擎的哈希值
		se.UpdataStorageEngine(db)
		return se.seHash, primaryProof, mptProof, nil
	} else if se.secondaryIndexMode == "meht" {
		newValues, mehtProof := se.InsertIntoMEHT(reversedKV)
		//打印插入结果
		fmt.Printf("key=%x , value=%x已插入非主键索引MEHT\n", []byte(reversedKV.GetKey()), []byte(newValues))
		meht.PrintMEHTProof(mehtProof)
		return se.seHash, primaryProof, nil, mehtProof
	} else {
		fmt.Printf("非主键索引类型siMode设置错误\n")
		return se.seHash, nil, nil, nil
	}
}

func (se *StorageEngine) InsertIntoMPT(kvpair *util.KVPair) (string, *mpt.MPTProof) {
	//插入非主键索引
	//先查询得到原有value与待插入value合并
	values, mptProof := se.secondaryIndex_mpt.QueryByKey(kvpair.GetKey())
	//用原有values构建待插入的kvpair
	insertedKV := util.NewKVPair(kvpair.GetKey(), values)
	//将新的value插入到kvpair中
	isExist := insertedKV.AddValue(kvpair.GetValue())
	//如果原有values中没有此value，则插入到mpt中
	if isExist {
		se.secondaryIndex_mpt.Insert(insertedKV)
		newValues, newProof := se.secondaryIndex_mpt.QueryByKey(insertedKV.GetKey())
		return newValues, newProof
	}
	return values, mptProof
}

func (se *StorageEngine) InsertIntoMEHT(kvpair *util.KVPair) (string, *meht.MEHTProof) {
	//插入非主键索引
	//先查询得到原有value与待插入value合并
	values, mehtProof := se.secondaryIndex_meht.QueryByKey(kvpair.GetKey())
	//用原有values构建待插入的kvpair
	insertedKV := util.NewKVPair(kvpair.GetKey(), values)
	//将新的value插入到kvpair中
	isExist := insertedKV.AddValue(kvpair.GetValue())
	//如果原有values中没有此value，则插入到mpt中
	if isExist {
		_, newValues, newProof := se.secondaryIndex_meht.Insert(insertedKV)
		return newValues, newProof
	}
	return values, mehtProof
}

// 打印查询结果
func PrintQueryResult(key string, value string, mptProof *mpt.MPTProof) {
	fmt.Printf("key=%s , value=%s\n", key, value)
	mptProof.PrintMPTProof()
}

func (se *StorageEngine) UpdataStorageEngine(db *leveldb.DB) []byte {
	//删除db中原有的se
	err := db.Delete(se.seHash, nil)
	if err != nil {
		fmt.Println("Delete StorageEngine from DB error:", err)
	}
	//更新seHash的哈希值
	var seHashs []byte
	seHashs = append(seHashs, se.primaryIndexRootHash...)
	if se.secondaryIndexMode == "mpt" {
		seHashs = append(seHashs, se.secondaryIndexRootHash_mpt...)
	}
	hash := sha256.Sum256(seHashs)
	se.seHash = hash[:]
	//将更新后的se写入db中
	err = db.Put(se.seHash, SerializeStorageEngine(se), nil)
	if err != nil {
		fmt.Println("Insert StorageEngine to DB error:", err)
	}
	return se.seHash
}

// 打印StorageEngine
func (se *StorageEngine) PrintStorageEngine() {
	fmt.Println("打印StorageEngine-------------------------------------------------------------------------------------------")
	fmt.Println("seHash:", se.seHash)
	fmt.Printf("primaryIndexRootHash:%x\n", se.primaryIndexRootHash)
	se.primaryIndex.PrintMPT()
	if se.secondaryIndexMode == "mpt" {
		fmt.Printf("secondaryIndexRootHash(mpt):%x\n", se.secondaryIndexRootHash_mpt)
		se.secondaryIndex_mpt.PrintMPT()
	} else if se.secondaryIndexMode == "meht" {
		// fmt.Printf("secondaryIndexRootHash(meht):%x\n", se.secondaryIndexRootHash_meht)
		se.secondaryIndex_meht.PrintMEHT()
	}
}

// 用于序列化StorageEngine的结构体
type SeStorageEngine struct {
	SeHash                     []byte //搜索引擎的哈希值，由主索引根哈希和辅助索引根哈希计算得到
	PrimaryIndexRootHash       []byte // 主键索引的根哈希值
	SecondaryIndexMode         string // 标识当前采用的非主键索引的类型，mpt或meht
	SecondaryIndexRootHash_mpt []byte //mpt类型的非主键索引根哈希
}

func SerializeStorageEngine(se *StorageEngine) []byte {
	sese := &SeStorageEngine{se.seHash, se.primaryIndexRootHash, se.secondaryIndexMode, se.secondaryIndexRootHash_mpt}
	jsonSE, err := json.Marshal(sese)
	if err != nil {
		fmt.Printf("SerializeStorageEngine error: %v\n", err)
		return nil
	}
	return jsonSE
}

func DeserializeStorageEngine(sestring []byte) (*StorageEngine, error) {
	var sese SeStorageEngine
	err := json.Unmarshal(sestring, &sese)
	if err != nil {
		fmt.Printf("DeserializeStorageEngine error: %v\n", err)
		return nil, err
	}
	se := &StorageEngine{sese.SeHash, nil, sese.PrimaryIndexRootHash, sese.SecondaryIndexMode, nil, sese.SecondaryIndexRootHash_mpt, nil}
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
