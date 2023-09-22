package meht

import (
	"MEHT/util"
	"strconv"
)

// import (
// 	"fmt"
// )

type SEH struct {
	gd  int // global depth, initial zero
	rdx int // rdx, initial  given

	bucketCapacity int // capacity of the bucket, initial given
	bucketSegNum   int // number of segment bits in the bucket, initial given

	ht            map[string]*Bucket // hash table of buckets
	bucketsNumber int                // number of buckets, initial zero
}

// newSEH returns a new SEH
func newSEH(rdx int, bc int, bs int) *SEH {
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
func (seh *SEH) GetProof(key string) (string, []byte, [][]byte) {
	bucket := seh.GetBucketByKey(key)
	if bucket == nil {
		return "", nil, nil
	}
	return bucket.GetProof(key)
}

// Insert inserts the key-value pair into the SEH,返回插入的bucket指针,插入的value,segRootHash,proof
func (seh *SEH) Insert(kvpair util.KVPair) (*Bucket, string, []byte, [][]byte) {
	//判断是否为第一次插入
	if seh.bucketsNumber == 0 {
		//创建新的bucket
		bucket := NewBucket(0, seh.rdx, seh.bucketCapacity, seh.bucketSegNum)
		bucket.Insert(kvpair)
		seh.ht[""] = bucket
		seh.bucketsNumber++
		return bucket, kvpair.GetValue(), bucket.merkleTrees[""].GetRootHash(), bucket.merkleTrees[""].GetProof(0)
	}
	//不是第一次插入,根据key和GD找到待插入的bucket
	bucket := seh.GetBucketByKey(kvpair.GetKey())
	if bucket != nil {
		//插入(更新)到已存在的bucket中
		buckets := bucket.Insert(kvpair)
		if len(buckets) == 1 {
			//未发生分裂
			return buckets[0], kvpair.GetValue(), buckets[0].merkleTrees[""].GetRootHash(), buckets[0].merkleTrees[""].GetProof(0)
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
			bkey := buckets[i].GetBucketKey()
			seh.ht[bkey] = buckets[i]
		}
		kvbucket := seh.GetBucketByKey(kvpair.GetKey())
		insertedV, segRootHash, proof := kvbucket.GetProof(kvpair.GetKey())
		return kvbucket, insertedV, segRootHash, proof
	}
	return nil, "", nil, nil
}
