package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"reflect"
	"strconv"
	"sync"
)

type MEHT struct {
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
	cache          *[]interface{} // cache[0],cache[1],cache[2],cache[3] represent cache of mgtNode,bucket,segment and merkleTree respectively
	cacheEnable    bool
	latch          sync.RWMutex
	sehUpdateLatch sync.Mutex
	mgtUpdateLatch sync.Mutex
}

// NewMEHT returns a new MEHT
func NewMEHT(rdx int, bc int, bs int, db *leveldb.DB, mgtNodeCC int, bucketCC int, segmentCC int, merkleTreeCC int, cacheEnable bool) *MEHT {
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
		c := make([]interface{}, 0)
		c = append(c, lMgtNode, lBucket, lSegment, lMerkleTree)
		return &MEHT{rdx, bc, bs, NewSEH(rdx, bc, bs), NewMGT(rdx), nil,
			&c, cacheEnable, sync.RWMutex{}, sync.Mutex{}, sync.Mutex{}}
	} else {
		return &MEHT{rdx, bc, bs, NewSEH(rdx, bc, bs), NewMGT(rdx), nil,
			nil, cacheEnable, sync.RWMutex{}, sync.Mutex{}, sync.Mutex{}}
	}
}

func (meht *MEHT) GetMgtHash() []byte {
	return meht.mgtHash
}

// UpdateMEHTToDB 更新MEHT到db
func (meht *MEHT) UpdateMEHTToDB(newRootHash []byte, db *leveldb.DB) {
	newHash := sha256.Sum256(newRootHash)
	meht.latch.Lock()
	oldHash := sha256.Sum256(meht.mgtHash)
	meht.mgtHash = make([]byte, len(newHash))
	copy(meht.mgtHash, newHash[:])
	if err := db.Delete(oldHash[:], nil); err != nil {
		panic(err)
	}
	newHash = sha256.Sum256(meht.mgtHash)
	meMEHT := SerializeMEHT(meht)
	meht.latch.Unlock()
	if err := db.Put(newHash[:], meMEHT, nil); err != nil {
		panic(err)
	}
}

// GetSEH returns the SEH of the MEHT
func (meht *MEHT) GetSEH(db *leveldb.DB) *SEH {
	meht.latch.RLock()
	defer meht.latch.RUnlock()
	for meht.seh == nil && meht.sehUpdateLatch.TryLock() {
		if meht.seh != nil {
			meht.sehUpdateLatch.Unlock()
			return meht.seh
		}
		if sehString, err := db.Get([]byte("seh"), nil); err == nil {
			seh, _ := DeserializeSEH(sehString)
			meht.seh = seh
		}
		meht.sehUpdateLatch.Unlock()
	}
	for meht.seh == nil {
	}
	return meht.seh
}

// GetMGT returns the MGT of the MEHT
func (meht *MEHT) GetMGT(db *leveldb.DB) *MGT {
	meht.latch.RLock()
	defer meht.latch.RUnlock()
	for meht.mgt == nil && meht.mgtUpdateLatch.TryLock() {
		if meht.mgt != nil {
			meht.mgtUpdateLatch.Unlock()
			return meht.mgt
		}
		if mgtString, err := db.Get(meht.mgtHash, nil); err == nil {
			mgt, _ := DeserializeMGT(mgtString)
			meht.mgt = mgt
		}
		meht.mgtUpdateLatch.Unlock()
	}
	for meht.mgt == nil {
	}
	return meht.mgt
}

// GetCache returns the cache of MEHT
func (meht *MEHT) GetCache() *[]interface{} {
	return meht.cache
}

// Insert inserts the key-value pair into the MEHT,返回插入的bucket指针,插入的value,segRootHash,segProof,mgtRootHash,mgtProof
func (meht *MEHT) Insert(kvPair util.KVPair, db *leveldb.DB, isDelete bool) (*Bucket, string, *MEHTProof) {
	//判断是否为第一次插入
	for meht.GetSEH(db).bucketsNumber == 0 && meht.seh.latch.TryLock() {
		if meht.seh.bucketsNumber != 0 || isDelete {
			meht.seh.latch.Unlock()
			break
		}
		//插入KV到SEH,第一个插入的操作一定不能是删除操作,因为此时root还没有bucket
		bucketSs, _, _, _ := meht.seh.Insert(kvPair, db, meht.cache, false)
		//新建mgt的根节点
		meht.mgt.Root = NewMGTNode(nil, true, bucketSs[0][0], db, meht.rdx, meht.cache)
		//更新mgt的根节点哈希并更新到db
		meht.mgtHash = meht.mgt.UpdateMGTToDB(db)
		//统计访问频次
		meht.mgt.UpdateHotnessList("new", util.IntArrayToString(meht.mgt.Root.bucketKey, meht.rdx), 1, nil)
		//mgtRootHash, mgtProof := meht.mgt.GetProof(bucketSs[0][0].GetBucketKey(), db, meht.cache)
		//return bucketSs[0][0], kvPair.GetValue(), &MEHTProof{merkleTree_.GetRootHash(), merkleTree_.GetProof(0), mgtRootHash, mgtProof}
		meht.seh.latch.Unlock()
		return bucketSs[0][0], kvPair.GetValue(), nil
	}
	for meht.seh.bucketsNumber == 0 { // 等待最初的桶建成
	}
	//不是第一次插入,根据key和GD找到待插入的bucket
	var bucketDelegationCode_ BucketDelegationCode = FAILED
	var bucketSs [][]*Bucket
	var timestamp *int64 //特定桶当前时间戳，可能是正在执行插入的线程开始接收委托的时间戳，也可能是 0
	//Client等待Delegate完成桶插入时桶的时间戳，在Delegate做完后waitTimestamp一定会与timestamp的值不同，
	//即使实际的桶已经不是timestamp所在的桶了，因为非零时间戳永远是递增的，因此可以用来判断Delegate是否已经完成了插入
	//因为无论是桶时间戳的改变还是换了一个新桶，这都意味着上一次插入已经完成了
	var waitTimestamp int64
	for bucketDelegationCode_ == FAILED {
		bucketSs, bucketDelegationCode_, timestamp, waitTimestamp = meht.seh.Insert(kvPair, db, meht.cache, isDelete)
	}
	// 要不返回需要上锁的那个桶，然后延迟释放桶锁，直到mgt更新完毕，这样子就可以从mgt粒度锁缩小至桶粒度
	if bucketDelegationCode_ == DELEGATE {
		//只有被委托线程需要更新mgt
		meht.mgt.MGTUpdate(bucketSs, db, meht.cache)
	}
	//获取当前KV插入的bucket
	for {
		if bucketDelegationCode_ == CLIENT && *timestamp == waitTimestamp { //Client等待Delegate完成插入，此处使用时间戳可以避免ht的读写冲突
			continue
		}
		meht.seh.latch.RLock()
		kvBucket := meht.seh.GetBucketByKey(kvPair.GetKey(), db, meht.cache) // 此处重新查询了插入值所在bucket
		meht.seh.latch.RUnlock()
		return kvBucket, kvPair.GetValue(), nil
	}
}

// MGTBatchCommit 批量提交mgt
func (meht *MEHT) MGTBatchCommit(db *leveldb.DB) {
	// meht存在则mgt一定存在，因此无需判断mgt是否为nil
	meht.mgt.MGTUpdate(nil, db, meht.cache)
	meht.mgt.UpdateMGTToDB(db)
	meht.UpdateMEHTToDB(meht.mgt.mgtRootHash, db)
}

// MGTCacheAdjust 缓存调整mgt
func (meht *MEHT) MGTCacheAdjust(db *leveldb.DB, a float64, b float64) {
	if !meht.mgt.IsNeedCacheAdjust(meht.GetSEH(db).bucketsNumber, a, b) {
		return
	}
	meht.mgt.CacheAdjust(db, meht.cache)
}

// PrintMEHT 打印整个MEHT
func (meht *MEHT) PrintMEHT(db *leveldb.DB) {
	fmt.Printf("打印MEHT-------------------------------------------------------------------------------------------\n")
	fmt.Printf("MEHT: rdx=%d, bucketCapacity=%d, bucketSegNum=%d\n", meht.rdx, meht.bc, meht.bs)
	meht.GetSEH(db).PrintSEH(db, meht.cache)
	meht.GetMGT(db).PrintMGT(db, meht.cache)
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

// QueryValueByKey 给定一个key，返回它的value及其用于查找证明的信息，包括segKey，seg是否存在，在seg中的index，不存在，则返回nil,nil
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
		value, segKey, isSegExist, index := bucket.GetValueByKey(key, db, meht.cache, false)
		return value, bucket, segKey, isSegExist, index
	}
	seh.latch.RUnlock()
	return "", nil, "", false, -1
}

// GetQueryProof 根据查询结果构建MEHTProof, mgtNode.bucket.rdx
func (meht *MEHT) GetQueryProof(bucket *Bucket, segKey string, isSegExist bool, index int, db *leveldb.DB) *MEHTProof {
	//找到segRootHash和segProof
	segRootHash, mhtProof := bucket.GetProof(segKey, isSegExist, index, db, meht.cache)
	//根据key找到mgtRootHash和mgtProof
	meht.GetMGT(db) // 保证 mgt 不为空
	meht.mgt.latch.RLock()
	mgtRootHash, mgtProof := meht.mgt.GetProof(bucket.GetBucketKey(), db, meht.cache)
	meht.mgt.latch.RUnlock()
	return &MEHTProof{segRootHash, mhtProof, mgtRootHash, mgtProof}
}

// PurgeCache 将Cache中所有数据都更新到磁盘上
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

// VerifyQueryResult 验证查询结果
func VerifyQueryResult(value string, mehtProof *MEHTProof) bool {
	//验证segProof
	//计算segRootHash
	//如果key不存在，则判断segment是否存在，存在则根据segment中所有的值构建segment的默克尔树根
	var segRootHash []byte
	if !mehtProof.mhtProof.GetIsExist() {
		if mehtProof.mhtProof.GetIsSegExist() {
			//key不存在，segment存在
			//var data [][]byte
			data := make([][]byte, 0)
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
		segRootHash = ComputeSegHashRoot(value, mehtProof.mhtProof.GetProofPairs())
		if segRootHash == nil || !bytes.Equal(segRootHash, mehtProof.segRootHash) {
			//fmt.Printf("segRootHash=%s计算错误,验证不通过\n", hex.EncodeToString(segRootHash))
			return false
		}
	}
	//计算mgtRootHash
	mgtRootHash := ComputeMGTRootHash(segRootHash, mehtProof.mgtProof)
	if mgtRootHash == nil || !bytes.Equal(mgtRootHash, mehtProof.mgtRootHash) {
		//fmt.Printf("mgtRootHash=%s计算错误,验证不通过\n", hex.EncodeToString(mgtRootHash))
		return false
	}
	return true
}

// PrintMEHTProof 打印MEHTProof
func (mehtProof *MEHTProof) PrintMEHTProof() {
	fmt.Printf("打印MEHTProof-------------------------------------------------------------------------------------------\n")
	fmt.Printf("segRootHash=%s\n", hex.EncodeToString(mehtProof.segRootHash))
	fmt.Printf("mhtProof:\n")
	PrintMHTProof(mehtProof.mhtProof)
	fmt.Printf("mgtRootHash=%s\n", hex.EncodeToString(mehtProof.mgtRootHash))
	fmt.Printf("mgtProof:\n")
	PrintMGTProof(mehtProof.mgtProof)
}

type SeMEHT struct {
	Rdx     int    // radix of one bit, initial  given，key编码的进制数（基数）
	Bc      int    // bucket capacity, initial given，每个bucket的容量
	Bs      int    // bucket segment number, initial given，key中区分segment的位数
	MgtHash []byte // hash of the mgt
}

// SerializeMEHT 序列化MEHT
func SerializeMEHT(meht *MEHT) []byte {
	seMEHT := &SeMEHT{meht.rdx, meht.bc, meht.bs, meht.mgtHash}
	if jsonSSN, err := json.Marshal(seMEHT); err != nil {
		fmt.Printf("SerializeMEHT error: %v\n", err)
		return nil
	} else {
		return jsonSSN
	}
}

// DeserializeMEHT 反序列化MEHT
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
		c := make([]interface{}, 0)
		c = append(c, lMgtNode, lBucket, lSegment, lMerkleTree)
		meht = &MEHT{seMEHT.Rdx, seMEHT.Bc, seMEHT.Bs, nil, nil, seMEHT.MgtHash,
			&c, cacheEnable, sync.RWMutex{}, sync.Mutex{}, sync.Mutex{}}
	} else {
		meht = &MEHT{seMEHT.Rdx, seMEHT.Bc, seMEHT.Bs, nil, nil, seMEHT.MgtHash,
			nil, cacheEnable, sync.RWMutex{}, sync.Mutex{}, sync.Mutex{}}
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
