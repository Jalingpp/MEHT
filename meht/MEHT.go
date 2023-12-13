package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

//NewMEHT(rdx int, bc int, bs int) *MEHT {}: NewMEHT returns a new MEHT
//Insert(kvpair util.KVPair) (*Bucket, string, *MEHTProof) {}: Insert inserts the key-value pair into the MEHT,返回插入的bucket指针,插入的value,segRootHash,segProof,mgtRootHash,mgtProof
//PrintMEHT() {}: 打印整个MEHT
//QueryByKey(key string) (string, MEHTProof) {}: 给定一个key，返回它的value及其证明proof，不存在，则返回nil,nil
//PrintQueryResult(key string, value string, mehtProof MEHTProof) {}: 打印查询结果
//VerifyQueryResult(value string, mehtProof MEHTProof) bool {}: 验证查询结果
//PrintMEHTProof(mehtProof MEHTProof) {}: 打印MEHTProof

type MEHT struct {
	name string // name of the MEHT, is used to distinguish different MEHTs in leveldb

	rdx int // radix of one bit, initial  given，key编码的进制数（基数）
	bc  int // bucket capacity, initial given，每个bucket的容量
	bs  int // bucket segment number, initial given，key中区分segment的位数

	seh     *SEH //the key of seh in leveldb is always name+"seh"
	mgt     *MGT
	mgtHash []byte // hash of the mgt, equals to the hash of the mgt root node hash, is used for index mgt in leveldb
}

// NewMEHT returns a new MEHT
func NewMEHT(name string, rdx int, bc int, bs int) *MEHT {
	return &MEHT{name, rdx, bc, bs, NewSEH(name, rdx, bc, bs), NewMGT(rdx), nil}
}

// 更新MEHT到db
func (meht *MEHT) UpdateMEHTToDB(db *leveldb.DB) {
	meMEHT := SerializeMEHT(meht)
	db.Put([]byte(meht.name+"meht"), meMEHT, nil)
}

// GetSEH returns the SEH of the MEHT
func (meht *MEHT) GetSEH(db *leveldb.DB) *SEH {
	if meht.seh == nil {
		sehString, error := db.Get([]byte(meht.name+"seh"), nil)
		if error == nil {
			seh, _ := DeserializeSEH(sehString)
			meht.seh = seh
		}
	}
	return meht.seh
}

// GetMGT returns the MGT of the MEHT
func (meht *MEHT) GetMGT(db *leveldb.DB) *MGT {
	if meht.mgt == nil {
		mgtString, error := db.Get(meht.mgtHash, nil)
		if error == nil {
			mgt, _ := DeserializeMGT(mgtString)
			meht.mgt = mgt
		}
	}
	return meht.mgt
}

// Insert inserts the key-value pair into the MEHT,返回插入的bucket指针,插入的value,segRootHash,segProof,mgtRootHash,mgtProof
func (meht *MEHT) Insert(kvpair *util.KVPair, db *leveldb.DB) (*Bucket, string, *MEHTProof) {
	//判断是否为第一次插入
	if meht.GetSEH(db).bucketsNumber == 0 {
		//插入KV到SEH
		buckets, _, segRootHash, mhtProof, _ := meht.seh.Insert(kvpair, db)
		//更新seh到db
		meht.seh.UpdateSEHToDB(db)
		//新建mgt的根节点
		meht.mgt.Root = NewMGTNode(nil, true, buckets[0], db, meht.rdx)
		//更新mgt的根节点哈希并更新到db
		meht.mgtHash = meht.mgt.UpdateMGTToDB(db)
		mgtRootHash, mgtProof := meht.mgt.GetProof(buckets[0].GetBucketKey(), db)
		return buckets[0], kvpair.GetValue(), &MEHTProof{segRootHash, mhtProof, mgtRootHash, mgtProof}
	}
	//不是第一次插入,根据key和GD找到待插入的bucket
	buckets, _, segRootHash, mhtProof, isInserted := meht.seh.Insert(kvpair, db)
	//无论是否分裂，都需要更新mgt
	meht.mgt = meht.GetMGT(db).MGTUpdate(buckets, db)
	//如果未插入，则重新执行上述两步（需要再向下分裂）
	for !isInserted {
		buckets, _, segRootHash, mhtProof, isInserted = meht.seh.Insert(kvpair, db)
		meht.mgt = meht.GetMGT(db).MGTUpdate(buckets, db)
	}
	//更新seh到db
	meht.seh.UpdateSEHToDB(db)
	//获取当前KV插入的bucket和插入证明
	kvbucket := meht.seh.GetBucketByKey(kvpair.GetKey(), db)
	mgtRootHash, mgtProof := meht.mgt.GetProof(kvbucket.GetBucketKey(), db)
	//更新mgt的根节点哈希并更新到db
	meht.mgtHash = meht.mgt.UpdateMGTToDB(db)
	return kvbucket, kvpair.GetValue(), &MEHTProof{segRootHash, mhtProof, mgtRootHash, mgtProof}
}

func (meht *MEHT) MGTCachedAdjust(db *leveldb.DB) {
	meht.mgtHash = meht.mgt.CacheAdjust(db)
	meht.UpdateMEHTToDB(db)
}

// 打印整个MEHT
func (meht *MEHT) PrintMEHT(db *leveldb.DB) {
	fmt.Printf("打印MEHT-------------------------------------------------------------------------------------------\n")
	fmt.Printf("MEHT: rdx=%d, bucketCapacity=%d, bucketSegNum=%d\n", meht.rdx, meht.bc, meht.bs)
	meht.GetSEH(db).PrintSEH(db)
	meht.GetMGT(db).PrintMGT(meht.name, db)
}

type MEHTProof struct {
	segRootHash []byte
	mhtProof    *mht.MHTProof
	mgtRootHash []byte
	mgtProof    []MGTProof
}

// 给定一个key，返回它的value及其用于查找证明的信息，包括segkey，seg是否存在，在seg中的index，不存在，则返回nil,nil
func (meht *MEHT) QueryValueByKey(key string, db *leveldb.DB) (string, *Bucket, string, bool, int) {
	//根据key找到bucket
	bucket := meht.GetSEH(db).GetBucketByKey(key, db)
	if bucket != nil {
		//根据key找到value
		value, segkey, isSegExist, index := bucket.GetValueByKey(key, db)
		return value, bucket, segkey, isSegExist, index
	}
	return "", nil, "", false, -1
}

// 根据查询结果构建MEHTProof, mgtNode.bucket.rdx
func (meht *MEHT) GetQueryProof(bucket *Bucket, segkey string, isSegExist bool, index int, db *leveldb.DB) *MEHTProof {
	//找到segRootHash和segProof
	segRootHash, mhtProof := bucket.GetProof(segkey, isSegExist, index, db)
	//根据key找到mgtRootHash和mgtProof
	mgtRootHash, mgtProof := meht.GetMGT(db).GetProof(bucket.GetBucketKey(), db)
	return &MEHTProof{segRootHash, mhtProof, mgtRootHash, mgtProof}
}

// 打印查询结果
func PrintQueryResult(key string, value string, mehtProof *MEHTProof) {
	fmt.Printf("查询结果-------------------------------------------------------------------------------------------\n")
	fmt.Printf("key=%s\n", key)
	if value == "" {
		fmt.Printf("value不存在\n")
	} else {
		fmt.Printf("value=%s\n", value)
	}
	PrintMEHTProof(mehtProof)
}

// 验证查询结果
func VerifyQueryResult(value string, mehtProof *MEHTProof) bool {
	//验证segProof
	fmt.Printf("验证查询结果-------------------------------------------------------------------------------------------\n")
	//计算segRootHash
	//如果key不存在，则判断segment是否存在，存在则根据segment中所有的值构建segment的默克尔树根
	var segRootHash []byte
	if !mehtProof.mhtProof.GetIsExist() {
		if mehtProof.mhtProof.GetIsSegExist() {
			//key不存在，segment存在
			var data [][]byte
			for i := 0; i < len(mehtProof.mhtProof.GetValues()); i++ {
				data = append(data, []byte(mehtProof.mhtProof.GetValues()[i]))
			}
			mht := mht.NewMerkleTree(data)
			segRootHash = mht.GetRootHash()
			if !bytes.Equal(segRootHash, mehtProof.segRootHash) {
				fmt.Printf("segRootHash=%s计算错误,验证不通过\n", hex.EncodeToString(mht.GetRootHash()))
				return false
			}
		} else {
			//key不存在，segment不存在，验证返回的所有segRootHashes是否在mgt叶节点中
			segRootHashes := mehtProof.mhtProof.GetSegRootHashes()
			for i := 0; i < len(segRootHashes); i++ {
				isIn := false
				for j := 0; j < len(mehtProof.mgtProof); j++ {
					if mehtProof.mgtProof[j].level > 0 {
						if isIn {
							break
						} else {
							fmt.Printf("MHTProof中的segRootHashes不在MGTProof的叶节点中,验证不通过\n")
							return false
						}
					}
					if bytes.Equal(segRootHashes[i], mehtProof.mgtProof[j].dataHash) {
						isIn = true
					}
				}
			}
			if len(segRootHashes) == 0 {
				segRootHash = nil
			} else {
				segRootHash = segRootHashes[0]
			}
		}
	} else {
		//如果key存在，则根据key对应的value构建segment的默克尔树根
		segRootHash = ComputSegHashRoot(value, mehtProof.mhtProof.GetProofPairs())
		if segRootHash == nil || !bytes.Equal(segRootHash, mehtProof.segRootHash) {
			fmt.Printf("segRootHash=%s计算错误,验证不通过\n", hex.EncodeToString(segRootHash))
			return false
		}
	}
	//计算mgtRootHash
	mgtRootHash := ComputMGTRootHash(segRootHash, mehtProof.mgtProof)
	if mgtRootHash == nil || !bytes.Equal(mgtRootHash, mehtProof.mgtRootHash) {
		fmt.Printf("mgtRootHash=%s计算错误,验证不通过\n", hex.EncodeToString(mgtRootHash))
		return false
	}
	fmt.Printf("验证通过,MGT的根哈希为:%s\n", hex.EncodeToString(mgtRootHash))
	return true
}

// 打印MEHTProof
func PrintMEHTProof(mehtProof *MEHTProof) {
	fmt.Printf("打印MEHTProof-------------------------------------------------------------------------------------------\n")
	fmt.Printf("segRootHash=%s\n", hex.EncodeToString(mehtProof.segRootHash))
	fmt.Printf("mhtProof:\n")
	PrintMHTProof(mehtProof.mhtProof)
	fmt.Printf("mgtRootHash=%s\n", hex.EncodeToString(mehtProof.mgtRootHash))
	fmt.Printf("mgtProof:\n")
	PrintMGTProof(mehtProof.mgtProof)
}

type SeMEHT struct {
	Name string //name of meht

	Rdx int // radix of one bit, initial  given，key编码的进制数（基数）
	Bc  int // bucket capacity, initial given，每个bucket的容量
	Bs  int // bucket segment number, initial given，key中区分segment的位数

	MgtHash []byte // hash of the mgt
}

// 序列化MEHT
func SerializeMEHT(meht *MEHT) []byte {
	seMEHT := &SeMEHT{meht.name, meht.rdx, meht.bc, meht.bs, meht.mgtHash}
	jsonSSN, err := json.Marshal(seMEHT)
	if err != nil {
		fmt.Printf("SerializeMEHT error: %v\n", err)
		return nil
	}
	return jsonSSN
}

// 反序列化MEHT
func DeserializeMEHT(data []byte) (*MEHT, error) {
	var seMEHT SeMEHT
	err := json.Unmarshal(data, &seMEHT)
	if err != nil {
		fmt.Printf("DeserializeMEHT error: %v\n", err)
		return nil, err
	}
	meht := &MEHT{seMEHT.Name, seMEHT.Rdx, seMEHT.Bc, seMEHT.Bs, nil, nil, seMEHT.MgtHash}
	return meht, nil
}

func (meht *MEHT) SetSEH(seh *SEH) {
	meht.seh = seh
}

func (meht *MEHT) SetMGT(mgt *MGT) {
	meht.mgt = mgt
}

// func main() {

//new a database
// dbPath := "data/levelDB/test"
// db, err := leveldb.OpenFile(dbPath, nil)
// if err != nil {
// 	fmt.Printf("OpenDB error: %v\n", err)
// 	return
// }

//测试MEHT
//创建一个bucket
// MEHT := meht.NewMEHT(mehtName, 2, 2, 1) //rdx,capacity,segNum

// var MEHT *meht.MEHT
// seTest := sedb.NewStorageEngine("meht", mehtName, 2, 2, 1)
// MEHT = seTest.GetSecondaryIndex_meht(db)
// if MEHT == nil {
// 	fmt.Printf("meht is nil, new meht\n")
// 	MEHT = meht.NewMEHT(mehtName, 2, 2, 1) //rdx, bc, bs
// }
// //创建4个KVPair
// kvpair1 := util.NewKVPair("0000", "value1")
// kvpair2 := util.NewKVPair("1001", "value2")
// kvpair3 := util.NewKVPair("0010", "value3")
// kvpair4 := util.NewKVPair("0000", "value4")

// MEHT.SetSEH(nil)
// MEHT.SetMGT(nil)

// MEHT.PrintMEHT(db)

// //插入kvpair1到MEHT
// MEHT.Insert(kvpair1, db)
// //打印整个MEHT
// MEHT.PrintMEHT(db)

// // 插入kvpair2到MEHT
// MEHT.Insert(kvpair2, db)
// // 打印整个MEHT
// MEHT.PrintMEHT(db)

// //插入kvpair3到MEHT
// MEHT.Insert(kvpair3, db)
// //打印整个MEHT
// MEHT.PrintMEHT(db)

// //插入kvpair4到MEHT
// MEHT.Insert(kvpair4, db)
// // //打印整个MEHT
// MEHT.PrintMEHT(db)

// MEHT.UpdateMEHTToDB(db)

// //查询kvpair1
// qv1, bucket1, segkey1, isSegExist1, index1 := MEHT.QueryValueByKey(kvpair1.GetKey(), db)
// //获取查询证明
// qpf1 := MEHT.GetQueryProof(bucket1, segkey1, isSegExist1, index1, db)
// //打印查询结果
// meht.PrintQueryResult(kvpair1.GetKey(), qv1, qpf1)
// //验证查询结果
// meht.VerifyQueryResult(qv1, qpf1)

// //查询kvpair1
// qv2, bucket2, segkey2, isSegExist2, index2 := MEHT.QueryValueByKey(kvpair2.GetKey(), db)
// //获取查询证明
// qpf2 := MEHT.GetQueryProof(bucket2, segkey2, isSegExist2, index2, db)
// //打印查询结果
// meht.PrintQueryResult(kvpair2.GetKey(), qv2, qpf2)
// //验证查询结果
// meht.VerifyQueryResult(qv2, qpf2)
// }
