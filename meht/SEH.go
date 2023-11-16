package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"strings"
)

// NewSEH(rdx int, bc int, bs int) *SEH {}:returns a new SEH
// GetBucketByKey(key string) *Bucket {}: returns the bucket with the given key
// GetValueByKey(key string) string {}: returns the value of the key-value pair with the given key
// GetProof(key string) (string, []byte, []mht.ProofPair) {}: returns the proof of the key-value pair with the given key
// Insert(kvpair util.KVPair) (*Bucket, string, []byte, [][]byte) {}: inserts the key-value pair into the SEH,返回插入的bucket指针,插入的value,segRootHash,proof
// PrintSEH() {}: 打印SEH

type SEH struct {
	name string //equals to the name of MEHT, is used to distinguish different SEH in leveldb

	gd  int // global depth, initial zero
	rdx int // rdx, initial  given

	bucketCapacity int // capacity of the bucket, initial given
	bucketSegNum   int // number of segment bits in the bucket, initial given

	ht            map[string]*Bucket // hash table of buckets
	bucketsNumber int                // number of buckets, initial zero
}

// newSEH returns a new SEH
func NewSEH(name string, rdx int, bc int, bs int) *SEH {
	return &SEH{name, 0, rdx, bc, bs, make(map[string]*Bucket), 0}
}

// 更新SEH到db
func (seh *SEH) UpdateSEHToDB(db *leveldb.DB) {
	seSEH := SerializeSEH(seh)
	if err := db.Put([]byte(seh.name+"seh"), seSEH, nil); err != nil {
		panic(err)
	}
}

// 获取bucket，如果内存中没有，从db中读取
func (seh *SEH) GetBucket(bucketKey string, db *leveldb.DB, cache *[]interface{}) *Bucket {
	ret := seh.ht[bucketKey]
	if ret == nil {
		var ok bool
		key_ := seh.name + "bucket" + bucketKey
		if cache != nil {
			targetCache, _ := (*cache)[1].(*lru.Cache[string, *Bucket])
			ret, ok = targetCache.Get(key_)
		}
		if !ok {
			if bucketString, error_ := db.Get([]byte(key_), nil); error_ == nil {
				bucket, _ := DeserializeBucket(bucketString)
				ret = bucket
			} else {
				ret = seh.GetBucket(bucketKey[util.ComputerStrideByBase(seh.rdx):], db, cache)
			}
		}
	}
	return ret
}

// GetBucket returns the bucket with the given key
func (seh *SEH) GetBucketByKey(key string, db *leveldb.DB, cache *[]interface{}) *Bucket {
	if seh.gd == 0 {
		return seh.ht[""]
	}
	var bkey string
	if len(key) >= seh.gd {
		bkey = key[len(key)-seh.gd*util.ComputerStrideByBase(seh.rdx):]
	} else {
		bkey = strings.Repeat("0", seh.gd*util.ComputerStrideByBase(seh.rdx)-len(key)) + key
	}
	return seh.GetBucket(bkey, db, cache)
	//不在ht中保存跳转桶
	//if seh.GetBucket(bkey, db) == nil {
	//	return nil
	//}
	//return seh.ht[bkey]
}

// GetGD returns the global depth of the SEH
func (seh *SEH) GetGD() int {
	return seh.gd
}

// GetHT returns the hash table of the SEH
func (seh *SEH) GetHT() map[string]*Bucket {
	return seh.ht
}

// GetBucketsNumber returns the number of buckets in the SEH
func (seh *SEH) GetBucketsNumber() int {
	return seh.bucketsNumber
}

// GetValue returns the value of the key-value pair with the given key
func (seh *SEH) GetValueByKey(key string, db *leveldb.DB, cache *[]interface{}) string {
	bucket := seh.GetBucketByKey(key, db, cache)
	if bucket == nil {
		return ""
	}
	return bucket.GetValue(key, db, cache)
}

// GetProof returns the proof of the key-value pair with the given key
func (seh *SEH) GetProof(key string, db *leveldb.DB, cache *[]interface{}) (string, []byte, *mht.MHTProof) {
	bucket := seh.GetBucketByKey(key, db, cache)
	value, segkey, isSegExist, index := bucket.GetValueByKey(key, db, cache)
	segRootHash, mhtProof := bucket.GetProof(segkey, isSegExist, index, db, cache)
	return value, segRootHash, mhtProof
}

// Insert inserts the key-value pair into the SEH,返回插入的bucket指针,插入的value,segRootHash,proof
func (seh *SEH) Insert(kvpair *util.KVPair, db *leveldb.DB, cache *[]interface{}) ([][]*Bucket, string, []byte, *mht.MHTProof) {
	//判断是否为第一次插入
	if seh.bucketsNumber == 0 {
		//创建新的bucket
		bucket := NewBucket(seh.name, 0, seh.rdx, seh.bucketCapacity, seh.bucketSegNum)
		bucket.Insert(kvpair, db, cache)
		seh.ht[""] = bucket
		//更新bucket到db
		bucket.UpdateBucketToDB(db, cache)
		seh.bucketsNumber++
		buckets := make([]*Bucket, 0)
		buckets = append(buckets, bucket)
		return [][]*Bucket{buckets}, kvpair.GetValue(), seh.ht[""].merkleTrees[bucket.GetSegmentKey(kvpair.GetKey())].GetRootHash(), seh.ht[""].merkleTrees[bucket.GetSegmentKey(kvpair.GetKey())].GetProof(0)
	}
	//不是第一次插入,根据key和GD找到待插入的bucket
	bucket := seh.GetBucketByKey(kvpair.GetKey(), db, cache)
	if bucket != nil {
		//插入(更新)到已存在的bucket中
		bucketss := bucket.Insert(kvpair, db, cache)
		newBucketsNumber := 0
		for _, b := range bucketss {
			newBucketsNumber += len(b)
		}
		seh.bucketsNumber += newBucketsNumber - len(bucketss)
		if newBucketsNumber == 1 {
			//未发生分裂
			//只将bucket[0][0]更新至db
			bk := bucketss[0][0]
			bk.UpdateBucketToDB(db, cache)
			return bucketss, kvpair.GetValue(), bk.merkleTrees[bk.GetSegmentKey(kvpair.GetKey())].GetRootHash(), bk.merkleTrees[bk.GetSegmentKey(kvpair.GetKey())].GetProof(0)
		}
		//发生分裂,更新ht
		//新的ld是第一层大多数bucket的ld+(层数-1),(层数-1)是连环分裂的次数
		newld := min(bucketss[0][0].GetLD(), bucketss[0][1].GetLD()) + len(bucketss) - 1
		seh.gd = max(seh.gd, newld)
		//无论是否扩展,均需遍历buckets,更新ht,更新buckets到db
		for i, buckets := range bucketss {
			for j := range buckets {
				if i != 0 && j == 0 { // 第一层往后每一层的第一个桶都是上一层分裂的那个桶，而上一层甚至更上层已经加过了，因此跳过
					continue
				}
				bkey := util.IntArrayToString(buckets[j].GetBucketKey(), buckets[j].rdx)
				seh.ht[bkey] = buckets[j]
				buckets[j].UpdateBucketToDB(db, cache)
			}

		}
		// ZYFCHANGE
		// 只在 seh 变动的位置将 seh 写入 db 可以省去很多重复写
		seh.UpdateSEHToDB(db)
		//
		kvbucket := seh.GetBucketByKey(kvpair.GetKey(), db, cache)
		insertedV, segkey, issegExist, index := kvbucket.GetValueByKey(kvpair.GetKey(), db, cache)
		segRootHash, proof := kvbucket.GetProof(segkey, issegExist, index, db, cache)
		return bucketss, insertedV, segRootHash, proof
	}
	return nil, "", nil, nil
}

// 打印SEH
func (seh *SEH) PrintSEH(db *leveldb.DB, cache *[]interface{}) {
	fmt.Printf("打印SEH-------------------------------------------------------------------------------------------\n")
	if seh == nil {
		return
	}
	fmt.Printf("SEH: gd=%d, rdx=%d, bucketCapacity=%d, bucketSegNum=%d, bucketsNumber=%d\n", seh.gd, seh.rdx, seh.bucketCapacity, seh.bucketSegNum, seh.bucketsNumber)
	for k := range seh.ht {
		fmt.Printf("bucketKey=%s\n", k)
		seh.GetBucket(k, db, cache).PrintBucket(db, cache)
	}
}

type SeSEH struct {
	Name string // seh name
	Gd   int    // global depth, initial zero
	Rdx  int    // rdx, initial  given

	BucketCapacity int // capacity of the bucket, initial given
	BucketSegNum   int // number of segment bits in the bucket, initial given

	HashTableKeys string // hash table of buckets
	BucketsNumber int    // number of buckets, initial zero
}

func SerializeSEH(seh *SEH) []byte {
	hashTableKeys := ""
	for k := range seh.ht {
		hashTableKeys += k + ","
	}
	seSEH := &SeSEH{seh.name, seh.gd, seh.rdx, seh.bucketCapacity, seh.bucketSegNum, hashTableKeys, seh.bucketsNumber}
	if jsonSEH, err := json.Marshal(seSEH); err != nil {
		fmt.Printf("SerializeSEH error: %v\n", err)
		return nil
	} else {
		return jsonSEH
	}
}

func DeserializeSEH(data []byte) (*SEH, error) {
	var seSEH SeSEH
	if err := json.Unmarshal(data, &seSEH); err != nil {
		fmt.Printf("DeserializeSEH error: %v\n", err)
		return nil, err
	}
	seh := &SEH{seSEH.Name, seSEH.Gd, seSEH.Rdx, seSEH.BucketCapacity, seSEH.BucketSegNum, make(map[string]*Bucket), seSEH.BucketsNumber}
	htKeys := strings.Split(seSEH.HashTableKeys, ",")
	for i := 0; i < len(htKeys); i++ {
		if htKeys[i] == "" && seSEH.Gd > 0 {
			continue
		} else {
			seh.ht[htKeys[i]] = nil
		}
	}
	return seh, nil
}

// import (
// 	"MEHT/meht"
// 	"MEHT/util"
// 	"encoding/hex"
// 	"fmt"
// )

// func main() {
//测试SEH
// var seh *meht.SEH
// mehtTest := meht.NewMEHT(mehtName, 2, 2, 1)
// mehtTest.SetSEH(nil)
// seh = mehtTest.GetSEH(db)
// if seh == nil {
// 	fmt.Printf("seh is nil, new seh\n")
// 	seh = meht.NewSEH(mehtName, 2, 2, 1) //rdx, bc, bs
// }

// //打印SEH
// seh.PrintSEH(db)

// kvpair1 := util.NewKVPair("1000", "value5")
// kvpair2 := util.NewKVPair("1001", "value6")
// // kvpair1 := util.NewKVPair("0000", "value1")
// // kvpair2 := util.NewKVPair("0001", "value2")
// kvpair3 := util.NewKVPair("0010", "value3")
// kvpair4 := util.NewKVPair("0011", "value4")

// //插入kvpair1
// _, insertedV1, _, _ := seh.Insert(kvpair1, db)
// //输出插入的bucketkey,插入的value,segRootHash,proof
// fmt.Printf("kvpair1 has been inserted into bucket %s: insertedValue=%s\n", util.IntArrayToString(seh.GetBucketByKey(kvpair1.GetKey(), db).GetBucketKey()), insertedV1)
// fmt.Printf("----------------------------------------------------------------------------------------------------------------------------\n")

// //插入kvpair1
// _, insertedV2, _, _ := seh.Insert(kvpair2, db)
// //输出插入的bucketkey,插入的value,segRootHash,proof
// fmt.Printf("kvpair2 has been inserted into bucket %s: insertedValue=%s\n", util.IntArrayToString(seh.GetBucketByKey(kvpair2.GetKey(), db).GetBucketKey()), insertedV2)
// fmt.Printf("----------------------------------------------------------------------------------------------------------------------------\n")

// //插入kvpair1
// _, insertedV3, _, _ := seh.Insert(kvpair3, db)
// //输出插入的bucketkey,插入的value,segRootHash,proof
// fmt.Printf("kvpair3 has been inserted into bucket %s: insertedValue=%s\n", util.IntArrayToString(seh.GetBucketByKey(kvpair3.GetKey(), db).GetBucketKey()), insertedV3)
// fmt.Printf("----------------------------------------------------------------------------------------------------------------------------\n")

// //插入kvpair1
// _, insertedV4, _, _ := seh.Insert(kvpair4, db)
// //输出插入的bucketkey,插入的value,segRootHash,proof
// fmt.Printf("kvpair4 has been inserted into bucket %s: insertedValue=%s\n", util.IntArrayToString(seh.GetBucketByKey(kvpair4.GetKey(), db).GetBucketKey()), insertedV4)
// fmt.Printf("----------------------------------------------------------------------------------------------------------------------------\n")

// //打印SEH
// seh.PrintSEH(db)

// seh.UpdateSEHToDB(db)

// }
