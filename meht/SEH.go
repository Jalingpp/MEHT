package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
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
	return &SEH{name, 0, rdx, bc, bs, make(map[string]*Bucket, 0), 0}
}

// 更新SEH到db
func (seh *SEH) UpdateSEHToDB(db *leveldb.DB) {
	seSEH := SerializeSEH(seh)
	db.Put([]byte(seh.name+"seh"), seSEH, nil)
}

// 获取bucket，如果内存中没有，从db中读取
func (seh *SEH) GetBucket(bucketKey string, db *leveldb.DB) *Bucket {
	if seh.ht[bucketKey] == nil {
		bucketString, error := db.Get([]byte(seh.name+"bucket"+bucketKey), nil)
		if error == nil {
			bucket, _ := DeserializeBucket(bucketString)
			seh.ht[bucketKey] = bucket
		}
	}
	return seh.ht[bucketKey]
}

// GetBucket returns the bucket with the given key
func (seh *SEH) GetBucketByKey(key string, db *leveldb.DB) *Bucket {
	if seh.gd == 0 {
		return seh.ht[""]
	}
	var bkey string
	if len(key) >= seh.gd {
		bkey = key[len(key)-seh.gd:]
	} else {
		bkey = strings.Repeat("0", seh.gd-len(key)) + key
	}
	if seh.GetBucket(bkey, db) == nil {
		return nil
	}
	return seh.ht[bkey]
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
func (seh *SEH) GetValueByKey(key string, db *leveldb.DB) string {
	bucket := seh.GetBucketByKey(key, db)
	if bucket == nil {
		return ""
	}
	return bucket.GetValue(key, db)
}

// GetProof returns the proof of the key-value pair with the given key
func (seh *SEH) GetProof(key string, db *leveldb.DB) (string, []byte, *mht.MHTProof) {
	bucket := seh.GetBucketByKey(key, db)
	value, segkey, isSegExist, index := bucket.GetValueByKey(key, db)
	segRootHash, mhtProof := bucket.GetProof(segkey, isSegExist, index, db)
	return value, segRootHash, mhtProof
}

// Insert inserts the key-value pair into the SEH,返回插入的bucket指针,插入的value,segRootHash,proof
func (seh *SEH) Insert(kvpair *util.KVPair, db *leveldb.DB) ([]*Bucket, string, []byte, *mht.MHTProof) {
	//判断是否为第一次插入
	if seh.bucketsNumber == 0 {
		//创建新的bucket
		bucket := NewBucket(seh.name, 0, seh.rdx, seh.bucketCapacity, seh.bucketSegNum)
		bucket.Insert(kvpair, db)
		seh.ht[""] = bucket
		//更新bucket到db
		bucket.UpdateBucketToDB(db)
		seh.bucketsNumber++
		buckets := make([]*Bucket, 0)
		buckets = append(buckets, bucket)
		return buckets, kvpair.GetValue(), seh.ht[""].merkleTrees[bucket.GetSegmentKey(kvpair.GetKey())].GetRootHash(), seh.ht[""].merkleTrees[bucket.GetSegmentKey(kvpair.GetKey())].GetProof(0)
	}
	//不是第一次插入,根据key和GD找到待插入的bucket
	bucket := seh.GetBucketByKey(kvpair.GetKey(), db)
	if bucket != nil {
		//插入(更新)到已存在的bucket中
		buckets := bucket.Insert(kvpair, db)
		seh.bucketsNumber += len(buckets) - 1
		if len(buckets) == 1 {
			//未发生分裂
			return buckets, kvpair.GetValue(), buckets[0].merkleTrees[buckets[0].GetSegmentKey(kvpair.GetKey())].GetRootHash(), buckets[0].merkleTrees[buckets[0].GetSegmentKey(kvpair.GetKey())].GetProof(0)
		}
		//发生分裂,更新ht
		newld := buckets[0].GetLD()
		if newld > seh.gd {
			//ht需扩展
			seh.gd++
			seh.ht = make(map[string]*Bucket, seh.gd*seh.rdx)
			//遍历orifinHT,将bucket指针指向新的bucket
			for k, _ := range seh.ht {
				for i := 0; i < seh.rdx; i++ {
					newbkey := strconv.Itoa(i) + k
					seh.ht[newbkey] = seh.GetBucket(k, db)
				}
			}
		}
		//无论是否扩展,均需遍历buckets,更新ht,更新buckets到db
		for i := 0; i < len(buckets); i++ {
			bkey := util.IntArrayToString(buckets[i].GetBucketKey())
			seh.ht[bkey] = buckets[i]
			buckets[i].UpdateBucketToDB(db)
		}
		kvbucket := seh.GetBucketByKey(kvpair.GetKey(), db)
		insertedV, segkey, issegExist, index := kvbucket.GetValueByKey(kvpair.GetKey(), db)
		segRootHash, proof := kvbucket.GetProof(segkey, issegExist, index, db)
		return buckets, insertedV, segRootHash, proof
	}
	return nil, "", nil, nil
}

// 打印SEH
func (seh *SEH) PrintSEH(db *leveldb.DB) {
	fmt.Printf("打印SEH-------------------------------------------------------------------------------------------\n")
	fmt.Printf("SEH: gd=%d, rdx=%d, bucketCapacity=%d, bucketSegNum=%d, bucketsNumber=%d\n", seh.gd, seh.rdx, seh.bucketCapacity, seh.bucketSegNum, seh.bucketsNumber)
	for k, b := range seh.ht {
		fmt.Printf("bucketKey=%s\n", k)
		b.PrintBucket(db)
	}
}

type SeSEH struct {
	Name string // seh name
	Gd   int    // global depth, initial zero
	Rdx  int    // rdx, initial  given

	BucketCapacity int // capacity of the bucket, initial given
	BucketSegNum   int // number of segment bits in the bucket, initial given

	HashTableKeys []string // hash table of buckets
	BucketsNumber int      // number of buckets, initial zero
}

func SerializeSEH(seh *SEH) []byte {
	seSEH := &SeSEH{seh.name, seh.gd, seh.rdx, seh.bucketCapacity, seh.bucketSegNum, make([]string, 0), seh.bucketsNumber}
	for k, _ := range seh.ht {
		seSEH.HashTableKeys = append(seSEH.HashTableKeys, k)
	}
	jsonSEH, err := json.Marshal(seSEH)
	if err != nil {
		fmt.Printf("SerializeSEH error: %v\n", err)
		return nil
	}
	return jsonSEH
}

func DeserializeSEH(data []byte) (*SEH, error) {
	var seSEH SeSEH
	err := json.Unmarshal(data, &seSEH)
	if err != nil {
		fmt.Printf("DeserializeSEH error: %v\n", err)
		return nil, err
	}
	seh := &SEH{seSEH.Name, seSEH.Gd, seSEH.Rdx, seSEH.BucketCapacity, seSEH.BucketSegNum, make(map[string]*Bucket, 0), seSEH.BucketsNumber}
	for i := 0; i < len(seSEH.HashTableKeys); i++ {
		seh.ht[seSEH.HashTableKeys[i]] = nil
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
// 	//测试SEH
// 	seh := meht.NewSEH(2, 2, 2) //rdx, bc, bs
// 	kvpair1 := util.NewKVPair("0000", "value1")
// 	kvpair2 := util.NewKVPair("0001", "value2")
// 	kvpair3 := util.NewKVPair("0010", "value3")
// 	kvpair4 := util.NewKVPair("0011", "value4")

// 	//插入kvpair1
// 	bucket1, insertedV1, segRootHash1, proof1 := seh.Insert(*kvpair1)
// 	//输出插入的bucketkey,插入的value,segRootHash,proof
// 	fmt.Printf("kvpair1 has been inserted into bucket %s: insertedValue=%s\n", bucket1.GetBucketKey(), insertedV1)
// 	fmt.Printf("segRootHash=%s\n", hex.EncodeToString(segRootHash1))
// 	proof1Str := "["
// 	for _, proof := range proof1 {
// 		proof1Str += hex.EncodeToString(proof) + ","
// 	}
// 	proof1Str = proof1Str[:len(proof1Str)-1] + "]"
// 	fmt.Printf("proof=%s\n", proof1Str)

// 	fmt.Printf("----------------------------------------------------------------------------------------------------------------------------\n")

// 	//插入kvpair2
// 	bucket2, insertedV2, segRootHash2, proof2 := seh.Insert(*kvpair2)
// 	//输出插入的bucketkey,插入的value,segRootHash,proof
// 	fmt.Printf("kvpair2 has been inserted into bucket %s: insertedValue=%s\n", bucket2.GetBucketKey(), insertedV2)
// 	fmt.Printf("segRootHash=%s\n", hex.EncodeToString(segRootHash2))
// 	proof2Str := "["
// 	for _, proof := range proof2 {
// 		proof2Str += hex.EncodeToString(proof) + ","
// 	}
// 	proof2Str = proof2Str[:len(proof2Str)-1] + "]"
// 	fmt.Printf("proof=%s\n", proof2Str)

// 	fmt.Printf("----------------------------------------------------------------------------------------------------------------------------\n")

// 	//插入kvpair3
// 	bucket3, insertedV3, segRootHash3, proof3 := seh.Insert(*kvpair3)
// 	//输出插入的bucketkey,插入的value,segRootHash,proof
// 	fmt.Printf("kvpair3 has been inserted into bucket %s: insertedValue=%s\n", bucket3.GetBucketKey(), insertedV3)
// 	fmt.Printf("segRootHash=%s\n", hex.EncodeToString(segRootHash3))
// 	proof3Str := "["
// 	for _, proof := range proof3 {
// 		proof3Str += hex.EncodeToString(proof) + ","
// 	}
// 	proof3Str = proof3Str[:len(proof3Str)-1] + "]"
// 	fmt.Printf("proof=%s\n", proof3Str)

// 	fmt.Printf("----------------------------------------------------------------------------------------------------------------------------\n")

// 	//插入kvpair4
// 	bucket4, insertedV4, segRootHash4, proof4 := seh.Insert(*kvpair4)
// 	//输出插入的bucketkey,插入的value,segRootHash,proof
// 	fmt.Printf("kvpair4 has been inserted into bucket %s: insertedValue=%s\n", bucket4.GetBucketKey(), insertedV4)
// 	fmt.Printf("segRootHash=%s\n", hex.EncodeToString(segRootHash4))
// 	proof4Str := "["
// 	for _, proof := range proof4 {
// 		proof4Str += hex.EncodeToString(proof) + ","
// 	}
// 	proof4Str = proof4Str[:len(proof4Str)-1] + "]"
// 	fmt.Printf("proof=%s\n", proof4Str)

// 	fmt.Printf("----------------------------------------------------------------------------------------------------------------------------\n")

// 	//打印SEH
// 	seh.PrintSEH()
// }
