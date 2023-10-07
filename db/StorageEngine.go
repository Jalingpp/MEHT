package db

import (
	"MEHT/meht"
	"MEHT/mpt"
	"MEHT/util"
	"fmt"
)

type StorageEngine struct {
	primaryIndex *mpt.MPT // mpt类型的主键索引

	secondaryIndexMode string // 标识当前采用的非主键索引的类型，mpt或meht

	secondaryIndex_mpt  *mpt.MPT   // mpt类型的非主键索引
	secondaryIndex_meht *meht.MEHT // meht类型的非主键索引
}

// NewStorageEngine() *StorageEngine: 返回一个新的StorageEngine
func NewStorageEngine(siMode string, rdx int, bc int, bs int) *StorageEngine {
	return &StorageEngine{mpt.NewMPT(), siMode, mpt.NewMPT(), meht.NewMEHT(rdx, bc, bs)}
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

// 向StorageEngine中插入一条记录
func (se *StorageEngine) Insert(kvpair *util.KVPair) {
	//插入主键索引
	se.primaryIndex.Insert(kvpair)
	fmt.Printf("key=%x , value=%x已插入主键索引MPT\n", []byte(kvpair.GetKey()), []byte(kvpair.GetValue()))
	//构造倒排KV
	reversedKV := util.ReverseKVPair(kvpair)
	//插入非主键索引
	if se.secondaryIndexMode == "mpt" {
		newValues, mptProof := se.InsertIntoMPT(reversedKV)
		//打印插入结果
		fmt.Printf("key=%x , value=%x已插入非主键索引MPT\n", []byte(reversedKV.GetKey()), []byte(newValues))
		mptProof.PrintMPTProof()
	} else if se.secondaryIndexMode == "meht" {
		newValues, mehtProof := se.InsertIntoMEHT(reversedKV)
		//打印插入结果
		fmt.Printf("key=%x , value=%x已插入非主键索引MEHT\n", []byte(reversedKV.GetKey()), []byte(newValues))
		meht.PrintMEHTProof(mehtProof)
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

// func main() {
// 	//测试StorageEngine
// 	//创建一个StorageEngine
// 	storageEngine := db.NewStorageEngine("meht")
// 	//读文件创建一个KVPair数组
// 	kvPairs := util.ReadKVPairFromFile("/home/jaling/Storage/index/meht/data/testdata.txt")
// 	//插入KVPair数组
// 	for i := 0; i < len(kvPairs); i++ {
// 		// fmt.Printf("key=%s,value=%s\n", kvPairs[i].GetKey(), kvPairs[i].GetValue())
// 		//把KVa转化为十六进制
// 		kvPairs[i].SetKey(util.StringToHex(kvPairs[i].GetKey()))
// 		kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
// 		// fmt.Printf("HexKey=%x,HexValue=%x\n", kvPairs[i].GetKey(), kvPairs[i].GetValue())
// 		storageEngine.Insert(kvPairs[i])
// 	}

// 	// fmt.Printf("len(kvPairs)=%d\n", len(kvPairs))

// 	// //打印主索引
// 	// fmt.Printf("打印主索引-------------------------------------------------------------------------------------------\n")
// 	// storageEngine.GetPrimaryIndex().PrintMPT()
// 	// //打印辅助索引
// 	// fmt.Printf("打印辅助索引-------------------------------------------------------------------------------------------\n")
// 	// storageEngine.GetSecondaryIndex_mpt().PrintMPT()

// 	//测试查询Bob
// 	fmt.Printf("测试查询Bob-------------------------------------------------------------------------------------------\n")
// 	qvBob, qvBobProof := storageEngine.GetSecondaryIndex_meht().QueryByKey(util.StringToHex("Evil"))
// 	// fmt.Printf("qvBob:%s\n", qvBob)
// 	qvs := strings.Split(qvBob, ",")
// 	fmt.Printf("key=%s查询结果：\n", "Evil")
// 	for i := 0; i < len(qvs); i++ {
// 		fmt.Printf("value=%s\n", util.HexToString(qvs[i]))
// 	}
// 	meht.PrintQueryResult("David", qvBob, qvBobProof)
// 	//验证查询结果
// 	// storageEngine.GetSecondaryIndex_mpt().VerifyQueryResult(qvBob, qvBobProof)
// 	meht.VerifyQueryResult(qvBob, qvBobProof)
// }
