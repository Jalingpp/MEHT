package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"fmt"
	"strconv"
)

// NewSEH(rdx int, bc int, bs int) *SEH {}:returns a new SEH
// GetBucketByKey(key string) *Bucket {}: returns the bucket with the given key
// GetValueByKey(key string) string {}: returns the value of the key-value pair with the given key
// GetProof(key string) (string, []byte, []mht.ProofPair) {}: returns the proof of the key-value pair with the given key
// Insert(kvpair util.KVPair) (*Bucket, string, []byte, [][]byte) {}: inserts the key-value pair into the SEH,返回插入的bucket指针,插入的value,segRootHash,proof
// PrintSEH() {}: 打印SEH

type SEH struct {
	gd  int // global depth, initial zero
	rdx int // rdx, initial  given

	bucketCapacity int // capacity of the bucket, initial given
	bucketSegNum   int // number of segment bits in the bucket, initial given

	ht            map[string]*Bucket // hash table of buckets
	bucketsNumber int                // number of buckets, initial zero
}

// newSEH returns a new SEH
func NewSEH(rdx int, bc int, bs int) *SEH {
	return &SEH{0, rdx, bc, bs, make(map[string]*Bucket, 0), 0}
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

// GetBucket returns the bucket with the given key
func (seh *SEH) GetBucketByKey(key string) *Bucket {
	if seh.gd == 0 {
		return seh.ht[""]
	}
	bkey := key[len(key)-seh.gd:]
	if seh.ht[bkey] == nil {
		return nil
	}
	return seh.ht[bkey]
}

// GetValue returns the value of the key-value pair with the given key
func (seh *SEH) GetValueByKey(key string) string {
	bucket := seh.GetBucketByKey(key)
	if bucket == nil {
		return ""
	}
	return bucket.GetValue(key)
}

// GetProof returns the proof of the key-value pair with the given key
func (seh *SEH) GetProof(key string) (string, []byte, *mht.MHTProof) {
	bucket := seh.GetBucketByKey(key)
	return bucket.GetProof(key)
}

// Insert inserts the key-value pair into the SEH,返回插入的bucket指针,插入的value,segRootHash,proof
func (seh *SEH) Insert(kvpair util.KVPair) (*Bucket, string, []byte, *mht.MHTProof) {
	//判断是否为第一次插入
	if seh.bucketsNumber == 0 {
		//创建新的bucket
		bucket := NewBucket(0, seh.rdx, seh.bucketCapacity, seh.bucketSegNum)
		bucket.Insert(kvpair)
		seh.ht[""] = bucket
		seh.bucketsNumber++
		return bucket, kvpair.GetValue(), seh.ht[""].merkleTrees[bucket.GetSegment(kvpair.GetKey())].GetRootHash(), seh.ht[""].merkleTrees[bucket.GetSegment(kvpair.GetKey())].GetProof(0)
	}
	//不是第一次插入,根据key和GD找到待插入的bucket
	bucket := seh.GetBucketByKey(kvpair.GetKey())
	if bucket != nil {
		//插入(更新)到已存在的bucket中
		buckets := bucket.Insert(kvpair)
		if len(buckets) == 1 {
			//未发生分裂
			return buckets[0], kvpair.GetValue(), buckets[0].merkleTrees[buckets[0].GetSegment(kvpair.GetKey())].GetRootHash(), buckets[0].merkleTrees[buckets[0].GetSegment(kvpair.GetKey())].GetProof(0)
		}
		//发生分裂,更新ht
		newld := buckets[0].GetLD()
		if newld > seh.gd {
			//ht需扩展
			seh.gd++
			originHT := seh.ht
			seh.ht = make(map[string]*Bucket, seh.gd*seh.rdx)
			//遍历orifinHT,将bucket指针指向新的bucket
			for k, v := range originHT {
				for i := 0; i < seh.rdx; i++ {
					newbkey := strconv.Itoa(i) + k
					seh.ht[newbkey] = v
				}
			}
		}
		//无论是否扩展,均需遍历buckets,更新ht
		for i := 0; i < len(buckets); i++ {
			bkey := util.IntArrayToString(buckets[i].GetBucketKey())
			seh.ht[bkey] = buckets[i]
		}
		kvbucket := seh.GetBucketByKey(kvpair.GetKey())
		insertedV, segRootHash, proof := kvbucket.GetProof(kvpair.GetKey())
		return kvbucket, insertedV, segRootHash, proof
	}
	return nil, "", nil, nil
}

// 打印SEH
func (seh *SEH) PrintSEH() {
	fmt.Printf("打印SEH-------------------------------------------------------------------------------------------\n")
	fmt.Printf("SEH: gd=%d, rdx=%d, bucketCapacity=%d, bucketSegNum=%d, bucketsNumber=%d\n", seh.gd, seh.rdx, seh.bucketCapacity, seh.bucketSegNum, seh.bucketsNumber)
	for k, v := range seh.ht {
		fmt.Printf("bucketKey=%s\n", k)
		v.PrintBucket()
	}
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
