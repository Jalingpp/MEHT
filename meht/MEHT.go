package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
)

//NewMEHT(rdx int, bc int, bs int) *MEHT {}: NewMEHT returns a new MEHT
//Insert(kvpair util.KVPair) (*Bucket, string, *MEHTProof) {}: Insert inserts the key-value pair into the MEHT,返回插入的bucket指针,插入的value,segRootHash,segProof,mgtRootHash,mgtProof
//PrintMEHT() {}: 打印整个MEHT
//QueryByKey(key string) (string, MEHTProof) {}: 给定一个key，返回它的value及其证明proof，不存在，则返回nil,nil
//PrintQueryResult(key string, value string, mehtProof MEHTProof) {}: 打印查询结果
//VerifyQueryResult(value string, mehtProof MEHTProof) bool {}: 验证查询结果
//PrintMEHTProof(mehtProof MEHTProof) {}: 打印MEHTProof

type MEHT struct {
	rdx int // radix of one bit, initial  given，key编码的进制数（基数）
	bc  int // bucket capacity, initial given，每个bucket的容量
	bs  int // bucket segment number, initial given，key中区分segment的位数

	seh *SEH
	mgt *MGT
}

// GetSEH returns the SEH of the MEHT
func (meht *MEHT) GetSEH() *SEH {
	return meht.seh
}

// GetMGT returns the MGT of the MEHT
func (meht *MEHT) GetMGT() *MGT {
	return meht.mgt
}

// NewMEHT returns a new MEHT
func NewMEHT(rdx int, bc int, bs int) *MEHT {
	return &MEHT{rdx, bc, bs, NewSEH(rdx, bc, bs), NewMGT()}
}

// Insert inserts the key-value pair into the MEHT,返回插入的bucket指针,插入的value,segRootHash,segProof,mgtRootHash,mgtProof
func (meht *MEHT) Insert(kvpair *util.KVPair) (*Bucket, string, *MEHTProof) {
	//判断是否为第一次插入
	if meht.seh.bucketsNumber == 0 {
		//创建新的bucket
		bucket := NewBucket(0, meht.seh.rdx, meht.seh.bucketCapacity, meht.seh.bucketSegNum)
		bucket.Insert(kvpair)
		//更新ht
		meht.seh.ht[""] = bucket
		meht.seh.bucketsNumber++
		//更新mgt
		meht.mgt.Root = NewMGTNode(nil, true, bucket)
		//获取proof
		_, segRootHash, mhtProof := meht.seh.ht[""].GetProof(kvpair.GetKey())
		mgtRootHash, mgtProof := meht.mgt.GetProof(bucket.GetBucketKey())
		return bucket, kvpair.GetValue(), &MEHTProof{segRootHash, mhtProof, mgtRootHash, mgtProof}
	}
	//不是第一次插入,根据key和GD找到待插入的bucket
	bucket := meht.seh.GetBucketByKey(kvpair.GetKey())
	if bucket != nil {
		//插入(更新)到已存在的bucket中
		buckets := bucket.Insert(kvpair)
		//无论是否分裂，都需要更新mgt
		meht.mgt.MGTUpdate(buckets)
		//未发生分裂
		if len(buckets) == 1 {
			//获取proof
			_, segRootHash, mhtProof := meht.seh.ht[kvpair.GetKey()[len(kvpair.GetKey())-meht.seh.gd:]].GetProof(kvpair.GetKey())
			mgtRootHash, mgtProof := meht.mgt.GetProof(bucket.GetBucketKey())
			return bucket, kvpair.GetValue(), &MEHTProof{segRootHash, mhtProof, mgtRootHash, mgtProof}
		}
		//发生分裂,更新ht
		newld := buckets[0].GetLD()
		if newld > meht.seh.gd {
			//ht需扩展
			meht.seh.gd++
			originHT := meht.seh.ht
			meht.seh.ht = make(map[string]*Bucket, meht.seh.gd*meht.seh.rdx)
			//遍历orifinHT,将bucket指针指向新的bucket
			for k, v := range originHT {
				for i := 0; i < meht.seh.rdx; i++ {
					newbkey := strconv.Itoa(i) + k
					meht.seh.ht[newbkey] = v
				}
			}
		}
		//无论是否扩展,均需遍历buckets,更新ht
		for i := 0; i < len(buckets); i++ {
			bkey := util.IntArrayToString(buckets[i].GetBucketKey())
			meht.seh.ht[bkey] = buckets[i]
		}
		_, segRootHash, mhtProof := meht.seh.ht[kvpair.GetKey()[len(kvpair.GetKey())-meht.seh.gd:]].GetProof(kvpair.GetKey())
		mgtRootHash, mgtProof := meht.mgt.GetProof(bucket.GetBucketKey())
		return bucket, kvpair.GetValue(), &MEHTProof{segRootHash, mhtProof, mgtRootHash, mgtProof}
	}
	return nil, "", &MEHTProof{nil, nil, nil, nil}
}

// 打印整个MEHT
func (meht *MEHT) PrintMEHT() {
	fmt.Printf("打印MEHT-------------------------------------------------------------------------------------------\n")
	fmt.Printf("MEHT: rdx=%d, bucketCapacity=%d, bucketSegNum=%d\n", meht.rdx, meht.bc, meht.bs)
	meht.seh.PrintSEH()
	meht.mgt.PrintMGT()
}

type MEHTProof struct {
	segRootHash []byte
	mhtProof    *mht.MHTProof
	mgtRootHash []byte
	mgtProof    []MGTProof
}

// 给定一个key，返回它的value及其证明proof，不存在，则返回nil,nil
func (meht *MEHT) QueryByKey(key string) (string, *MEHTProof) {
	//根据key找到bucket
	bucket := meht.seh.GetBucketByKey(key)
	if bucket != nil {
		//根据key找到segRootHash和segProof
		value, segRootHash, mhtProof := meht.seh.ht[key[len(key)-meht.seh.gd:]].GetProof(key)
		//根据key找到mgtRootHash和mgtProof
		mgtRootHash, mgtProof := meht.mgt.GetProof(bucket.GetBucketKey())
		return value, &MEHTProof{segRootHash, mhtProof, mgtRootHash, mgtProof}
	}
	return "", &MEHTProof{nil, nil, nil, nil}
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
	//fmt.Printf("验证查询结果-------------------------------------------------------------------------------------------\n")
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
				//fmt.Printf("segRootHash=%s计算错误,验证不通过\n", hex.EncodeToString(mht.GetRootHash()))
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
							//fmt.Printf("MHTProof中的segRootHashes不在MGTProof的叶节点中,验证不通过\n")
							return false
						}
					}
					if bytes.Equal(segRootHashes[i], mehtProof.mgtProof[j].dataHash) {
						isIn = true
					}
				}
			}
			segRootHash = segRootHashes[0]
		}
	} else {
		//如果key存在，则根据key对应的value构建segment的默克尔树根
		segRootHash = ComputSegHashRoot(value, mehtProof.mhtProof.GetProofPairs())
		if segRootHash == nil || !bytes.Equal(segRootHash, mehtProof.segRootHash) {
			//fmt.Printf("segRootHash=%s计算错误,验证不通过\n", hex.EncodeToString(segRootHash))
			return false
		}
	}
	//计算mgtRootHash
	mgtRootHash := ComputMGTRootHash(segRootHash, mehtProof.mgtProof)
	if mgtRootHash == nil || !bytes.Equal(mgtRootHash, mehtProof.mgtRootHash) {
		//fmt.Printf("mgtRootHash=%s计算错误,验证不通过\n", hex.EncodeToString(mgtRootHash))
		return false
	}
	//fmt.Printf("验证通过,MGT的根哈希为:%s\n", hex.EncodeToString(mgtRootHash))
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

// func main() {

// 	//测试MEHT
// 	//创建一个bucket
// 	MEHT := meht.NewMEHT(2, 2, 1) //rdx,capacity,segNum
// 	//创建4个KVPair
// 	kvpair1 := util.NewKVPair("0000", "value1")
// 	kvpair2 := util.NewKVPair("1001", "value2")
// 	kvpair3 := util.NewKVPair("0010", "value3")
// 	kvpair4 := util.NewKVPair("0000", "value4")

// 	// //插入kvpair1到MEHT
// 	// bucket1, insertedV1, MEHTProof1 := MEHT.Insert(*kvpair1)
// 	// bucket2, insertedV2, MEHTProof2 := MEHT.Insert(*kvpair2)
// 	MEHT.Insert(*kvpair1)
// 	MEHT.Insert(*kvpair2)
// 	MEHT.Insert(*kvpair3)
// 	MEHT.Insert(*kvpair4)

// 	//查询kvpair1
// 	qv1, qpf1 := MEHT.QueryByKey(kvpair1.GetKey())
// 	//打印查询结果
// 	meht.PrintQueryResult(kvpair1.GetKey(), qv1, qpf1)
// 	//验证查询结果
// 	meht.VerifyQueryResult(qv1, qpf1)

// 	//查询kvpair2
// 	qv2, qpf2 := MEHT.QueryByKey(kvpair2.GetKey())
// 	//打印查询结果
// 	meht.PrintQueryResult(kvpair2.GetKey(), qv2, qpf2)
// 	//验证查询结果
// 	meht.VerifyQueryResult(qv2, qpf2)

// 	//查询kvpair3
// 	qv3, qpf3 := MEHT.QueryByKey("1010")
// 	//打印查询结果
// 	meht.PrintQueryResult(kvpair3.GetKey(), qv3, qpf3)
// 	//验证查询结果
// 	meht.VerifyQueryResult(qv3, qpf3)

// 	//查询kvpair4
// 	qv4, qpf4 := MEHT.QueryByKey(kvpair4.GetKey())
// 	//打印查询结果
// 	meht.PrintQueryResult(kvpair4.GetKey(), qv4, qpf4)
// 	//验证查询结果
// 	meht.VerifyQueryResult(qv4, qpf4)

// 	//打印整个MEHT
// 	MEHT.PrintMEHT()
// }
