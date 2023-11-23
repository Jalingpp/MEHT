package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"reflect"
	"strconv"
	"sync"
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

	//mgtNodeCache         *lru.Cache[string, *MGTNode]
	//bucketCache          *lru.Cache[string, *Bucket]
	//segmentCache         *lru.Cache[string, *[]util.KVPair]
	// merkleTreeCache	   *lru.Cache[string, *mht.MerkleTree]
	cache       *[]interface{} // cache[0],cache[1],cache[2],cache[3] represent cache of mgtNode,bucket,segment and merkleTree respectively
	cacheEnable bool
	latch       sync.RWMutex
	updateLatch sync.Mutex
}

// NewMEHT returns a new MEHT
func NewMEHT(name string, rdx int, bc int, bs int, db *leveldb.DB, mgtNodeCC int, bucketCC int, segmentCC int, merkleTreeCC int, cacheEnable bool) *MEHT {
	if cacheEnable {
		lMgtNode, _ := lru.NewWithEvict[string, *MGTNode](mgtNodeCC, func(k string, v *MGTNode) {
			callBackFoo[string, *MGTNode](k, v, db)
		})
		lBucket, _ := lru.NewWithEvict[string, *Bucket](bucketCC, func(k string, v *Bucket) {
			callBackFoo[string, *Bucket](k, v, db)
		})
		lSegment, _ := lru.NewWithEvict[string, *[]util.KVPair](segmentCC, func(k string, v *[]util.KVPair) {
			callBackFoo[string, *[]util.KVPair](k, v, db)
		})
		lMerkleTree, _ := lru.NewWithEvict[string, *mht.MerkleTree](merkleTreeCC, func(k string, v *mht.MerkleTree) {
			callBackFoo[string, *mht.MerkleTree](k, v, db)
		})
		var c []interface{}
		c = append(c, lMgtNode, lBucket, lSegment, lMerkleTree)
		return &MEHT{name, rdx, bc, bs, NewSEH(name, rdx, bc, bs), NewMGT(rdx), nil,
			&c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	} else {
		return &MEHT{name, rdx, bc, bs, NewSEH(name, rdx, bc, bs), NewMGT(rdx), nil,
			nil, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	}
}

// 更新MEHT到db
func (meht *MEHT) UpdateMEHTToDB(db *leveldb.DB) {
	meht.latch.RLock()
	meMEHT := SerializeMEHT(meht)
	meht.latch.RUnlock()
	if err := db.Put([]byte(meht.name+"meht"), meMEHT, nil); err != nil {
		panic(err)
	}
}

// GetSEH returns the SEH of the MEHT
func (meht *MEHT) GetSEH(db *leveldb.DB) *SEH {
	meht.latch.RLock()
	defer meht.latch.RUnlock()
	if meht.seh == nil {
		sehString, error_ := db.Get([]byte(meht.name+"seh"), nil)
		if error_ == nil {
			seh, _ := DeserializeSEH(sehString)
			meht.updateLatch.Lock()
			meht.seh = seh
			meht.updateLatch.Unlock()
		}
	}
	return meht.seh
}

// GetMGT returns the MGT of the MEHT
func (meht *MEHT) GetMGT(db *leveldb.DB) *MGT {
	meht.latch.RLock()
	defer meht.latch.RUnlock()
	if meht.mgt == nil {
		mgtString, error_ := db.Get(meht.mgtHash, nil)
		if error_ == nil {
			mgt, _ := DeserializeMGT(mgtString)
			meht.updateLatch.Lock()
			meht.mgt = mgt
			meht.updateLatch.Unlock()
		}
	}
	return meht.mgt
}

// GetCache returns the cache of MEHT
func (meht *MEHT) GetCache() *[]interface{} {
	return meht.cache
}

// Insert inserts the key-value pair into the MEHT,返回插入的bucket指针,插入的value,segRootHash,segProof,mgtRootHash,mgtProof
func (meht *MEHT) Insert(kvpair *util.KVPair, db *leveldb.DB) (*Bucket, string, *MEHTProof) {
	//判断是否为第一次插入
	if meht.GetSEH(db).bucketsNumber == 0 && meht.seh.latch.TryLock() {
		defer meht.seh.latch.Unlock()
		//插入KV到SEH
		bucketss, _, _, _ := meht.seh.Insert(kvpair, db, meht.cache, nil)
		//merkleTree_ := meht.seh.ht[""].merkleTrees[bucketss[0][0].GetSegmentKey(kvpair.GetKey())]
		//更新seh到db
		meht.seh.UpdateSEHToDB(db)
		//新建mgt的根节点
		meht.mgt.Root = NewMGTNode(nil, true, bucketss[0][0], db, meht.rdx)
		//更新mgt的根节点哈希并更新到db
		meht.mgtHash = meht.mgt.UpdateMGTToDB(db)
		//mgtRootHash, mgtProof := meht.mgt.GetProof(bucketss[0][0].GetBucketKey(), db, meht.cache)
		//return bucketss[0][0], kvpair.GetValue(), &MEHTProof{merkleTree_.GetRootHash(), merkleTree_.GetProof(0), mgtRootHash, mgtProof}
		return bucketss[0][0], kvpair.GetValue(), nil
	}
	for meht.GetSEH(db).bucketsNumber == 0 { // 等待最初的桶建成
	}
	//不是第一次插入,根据key和GD找到待插入的bucket
	var bucketDelegationCode_ bucketDelegationCode = FAILED
	var bucketss [][]*Bucket
	var timestamp *int64 //特定桶当前时间戳，可能是正在执行插入的线程开始接收委托的时间戳，也可能是 0
	//Client等待Delegate完成桶插入时桶的时间戳，在Delegate做完后waitTimestamp一定会与timestamp的值不同，
	//即使实际的桶已经不是timestamp所在的桶了，因为非零时间戳永远是递增的，因此可以用来判断Delegate是否已经完成了插入
	//因为无论是桶时间戳的改变还是换了一个新桶，这都意味着上一次插入已经完成了
	var waitTimestamp int64
	for bucketDelegationCode_ == FAILED {
		bucketss, bucketDelegationCode_, timestamp, waitTimestamp = meht.seh.Insert(kvpair, db, meht.cache, &meht.mgt.latch)
	}
	if bucketDelegationCode_ == DELEGATE {
		//只有被委托线程需要更新mgt
		meht.mgt = meht.GetMGT(db).MGTUpdate(bucketss, db, meht.cache)
		//更新mgt的根节点哈希并更新到db
		meht.mgtHash = meht.mgt.UpdateMGTToDB(db)
		//fmt.Println("Try to unlock mgt latch in meht insert.")
		meht.mgt.latch.Unlock() // 内部只加锁不释放锁，保证委托线程工作完成后才释放锁
		//fmt.Println("Unlock successfully in meht insert.")
	}
	//获取当前KV插入的bucket
	for {
		if bucketDelegationCode_ == CLIENT && *timestamp == waitTimestamp { //Client等待Delegate完成插入，此处使用时间戳可以避免ht的读写冲突
			continue
		}
		//很有可能在获取读锁之前又有新的正式插入执行而获取到了mgt的写锁，那么这里就会被阻塞，
		//但这并不影响确实插入了这个东西的存在性证明，总会在某一个时刻找到一个当前时刻的位置的哈希与proof证明插入成功
		meht.mgt.latch.RLock() //读锁住mgt树根既保证阻塞新的插入保证桶位置不变，同时保证此时bucket对应mgtNode路径不会在寻找中途更改
		//meht.seh.latch.RLock() //这里其实可以不锁，因为只有桶插入后分裂才会引发seh更新，但是mgt锁住其实就已经不可能正式执行桶插入了
		kvbucket := meht.seh.GetBucketByKey(kvpair.GetKey(), db, meht.cache) // 此处重新查询了插入值所在bucket
		//meht.seh.latch.RUnlock()
		//kvbucket.latch.RLock() //这里其实可以不锁，因为mgt锁住其实就已经不可能有正式桶插入了，而且不锁的话可以在这部分获取proof的同时让其他插入线程继续在这个桶委托，等待一并插入
		//kvbucket.mtLatch.RLock() //这里其实可以不锁，因为只有桶插入才会引发树更新，但是mgt锁住其实就已经不可能正式执行桶插入了
		//merkleTree_ := kvbucket.merkleTrees[kvbucket.GetSegmentKey(kvpair.GetKey())]
		//segRootHash, mhtProof := merkleTree_.GetRootHash(), merkleTree_.GetProof(0)
		//mgtRootHash, mgtProof := meht.mgt.GetProof(kvbucket.GetBucketKey(), db, meht.cache) // 因此即使委托了数据，只要最终返回的proof能找到这个桶就好了，也不需要是最初被插入的那个
		//kvbucket.mtLatch.RUnlock()
		//kvbucket.latch.RUnlock()
		meht.mgt.latch.RUnlock()
		//return kvbucket, kvpair.GetValue(), &MEHTProof{segRootHash, mhtProof, mgtRootHash, mgtProof}
		return kvbucket, kvpair.GetValue(), nil
	}
}

// 打印整个MEHT
func (meht *MEHT) PrintMEHT(db *leveldb.DB) {
	fmt.Printf("打印MEHT-------------------------------------------------------------------------------------------\n")
	fmt.Printf("MEHT: rdx=%d, bucketCapacity=%d, bucketSegNum=%d\n", meht.rdx, meht.bc, meht.bs)
	meht.GetSEH(db).PrintSEH(db, meht.cache)
	meht.GetMGT(db).PrintMGT(meht.name, db, meht.cache)
}

type MEHTProof struct {
	segRootHash []byte
	mhtProof    *mht.MHTProof
	mgtRootHash []byte
	mgtProof    []MGTProof
}

func (mehtProof *MEHTProof) GetSizeOf() uint {
	ret := uint(len(mehtProof.segRootHash)+len(mehtProof.mgtRootHash)) * util.SIZEOFBYTE
	if mehtProof.mhtProof != nil {
		mehtProof.mhtProof.GetSizeOf()
	}
	for _, proof := range mehtProof.mgtProof {
		ret += proof.GetSizeOf()
	}
	return ret
}

// 给定一个key，返回它的value及其用于查找证明的信息，包括segkey，seg是否存在，在seg中的index，不存在，则返回nil,nil
func (meht *MEHT) QueryValueByKey(key string, db *leveldb.DB) (string, *Bucket, string, bool, int) {
	//根据key找到bucket
	//此处在锁了seh以后试图获取bucket读锁，但是刚好有插入获取了这个bucket的写锁然后分裂了，试图获取seh的写锁并更新，因此死锁
	meht.latch.RLock()
	defer meht.latch.RUnlock()
	seh := meht.GetSEH(db)
	seh.latch.RLock()
	if bucket := seh.GetBucketByKey(key, db, meht.cache); bucket != nil {
		//根据key找到value
		seh.latch.RUnlock() //防止在锁了seh以后试图获取bucket读锁，但是刚好有插入获取了这个bucket的写锁然后分裂了，试图获取seh的写锁并更新，导致死锁
		value, segkey, isSegExist, index := bucket.GetValueByKey(key, db, meht.cache)
		return value, bucket, segkey, isSegExist, index
	}
	seh.latch.RUnlock()
	return "", nil, "", false, -1
}

// 根据查询结果构建MEHTProof, mgtNode.bucket.rdx
func (meht *MEHT) GetQueryProof(bucket *Bucket, segkey string, isSegExist bool, index int, db *leveldb.DB) *MEHTProof {
	//找到segRootHash和segProof
	segRootHash, mhtProof := bucket.GetProof(segkey, isSegExist, index, db, meht.cache)
	//根据key找到mgtRootHash和mgtProof
	meht.GetMGT(db).latch.RLock()
	meht.mgt.latch.RLock()
	mgtRootHash, mgtProof := meht.GetMGT(db).GetProof(bucket.GetBucketKey(), db, meht.cache)
	meht.GetMGT(db).latch.RUnlock()
	return &MEHTProof{segRootHash, mhtProof, mgtRootHash, mgtProof}
}

// 将Cache中所有数据都更新到磁盘上
func (meht *MEHT) PurgeCache() {
	for idx, cache_ := range *(meht.cache) {
		switch idx {
		case 0:
			targetCache, _ := cache_.(*lru.Cache[string, *MGTNode])
			targetCache.Purge()
		case 1:
			targetCache, _ := cache_.(*lru.Cache[string, *Bucket])
			targetCache.Purge()
		case 2:
			targetCache, _ := cache_.(*lru.Cache[string, *[]util.KVPair])
			targetCache.Purge()
		case 3:
			targetCache, _ := cache_.(*lru.Cache[string, *mht.MerkleTree])
			targetCache.Purge()
		default:
			panic("Unknown cache type with idx " + strconv.Itoa(idx) + " in function PurgeCache.")
		}
	}
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
			mht_ := mht.NewMerkleTree(data)
			segRootHash = mht_.GetRootHash()
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
	if jsonSSN, err := json.Marshal(seMEHT); err != nil {
		fmt.Printf("SerializeMEHT error: %v\n", err)
		return nil
	} else {
		return jsonSSN
	}
}

// 反序列化MEHT
func DeserializeMEHT(data []byte, db *leveldb.DB, cacheEnable bool,
	mgtNodeCC int, bucketCC int, segmentCC int, merkleTreeCC int) (meht *MEHT, err error) {
	var seMEHT SeMEHT
	if err = json.Unmarshal(data, &seMEHT); err != nil {
		fmt.Printf("DeserializeMEHT error: %v\n", err)
		return
	}
	if cacheEnable {
		lMgtNode, _ := lru.NewWithEvict[string, *MGTNode](mgtNodeCC, func(k string, v *MGTNode) {
			callBackFoo[string, *MGTNode](k, v, db)
		})
		lBucket, _ := lru.NewWithEvict[string, *Bucket](bucketCC, func(k string, v *Bucket) {
			callBackFoo[string, *Bucket](k, v, db)
		})
		lSegment, _ := lru.NewWithEvict[string, *[]util.KVPair](segmentCC, func(k string, v *[]util.KVPair) {
			callBackFoo[string, *[]util.KVPair](k, v, db)
		})
		lMerkleTree, _ := lru.NewWithEvict[string, *mht.MerkleTree](merkleTreeCC, func(k string, v *mht.MerkleTree) {
			callBackFoo[string, *mht.MerkleTree](k, v, db)
		})
		var c []interface{}
		c = append(c, lMgtNode, lBucket, lSegment, lMerkleTree)
		meht = &MEHT{seMEHT.Name, seMEHT.Rdx, seMEHT.Bc, seMEHT.Bs, nil, nil, seMEHT.MgtHash,
			&c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	} else {
		meht = &MEHT{seMEHT.Name, seMEHT.Rdx, seMEHT.Bc, seMEHT.Bs, nil, nil, seMEHT.MgtHash,
			nil, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	}
	return
}

func (meht *MEHT) SetSEH(seh *SEH) {
	meht.seh = seh
}

func (meht *MEHT) SetMGT(mgt *MGT) {
	meht.mgt = mgt
}

func callBackFoo[K comparable, V any](k K, v V, db *leveldb.DB) {
	k_, err := util.ToStringE(k)
	if err != nil {
		panic(err)
	}
	var v_ []byte
	switch any(v).(type) {
	case *MGTNode:
		v_ = SerializeMGTNode(any(v).(*MGTNode))
	case *Bucket:
		v_ = SerializeBucket(any(v).(*Bucket))
	case *[]util.KVPair:
		v_ = SerializeSegment(*any(v).(*[]util.KVPair))
	case *mht.MerkleTree:
		v_ = mht.SerializeMHT(any(v).(*mht.MerkleTree))
	default:
		panic("Unknown type " + reflect.TypeOf(v).String() + " in callBackFoo of MEHT.")
	}
	if err = db.Put([]byte(k_), v_, nil); err != nil {
		panic(err)
	}
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
