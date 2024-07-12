package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
	"time"
)

type Bucket struct {
	BucketKey       []int    // bucket key, initial nil, related to ld and kvPair.key
	ld              int      // local depth, initial zero
	rdx             int      // rdx, initial 2
	capacity        int      // capacity of the bucket, initial given
	number          int      // number of data objects in the bucket, initial zero
	segNum          int      // number of segment bits in the bucket, initial given
	segments        sync.Map // segments: data objects in each segment  map[string][]util.KVPair
	segIdxMaps      sync.Map // segment index maps: map from key to index in segment map[string]map[string]int
	merkleTrees     sync.Map // merkle trees: one for each segment map[string]*mht.MerkleTree
	latchTimestamp  int64
	DelegationList  map[string]util.KVPair    // 委托插入的数据，用于后续一并插入，使用map结构是因为委托插入的数据可能键相同，需要通过key去找到并合并,map的key就是KVPair的key
	PendingNum      int                       // 委托插入的数据数量，用于新到来的委托方判断是否可以在被委托者正在进行桶插入操作时向DelegationList追加待插入数据，只有被委托者可以修改该字段
	toDelMap        map[string]map[string]int // 用于记录哪些数据需要被延迟删除
	latch           sync.RWMutex              //桶粒度锁
	segLatch        sync.RWMutex              //段粒度锁
	mtLatch         sync.RWMutex              //默克尔树锁
	DelegationLatch sync.Mutex                // 委托线程锁，用于将待插入数据更新到委托插入数据集，当被委托线程获取到MGT树根写锁时，也会试图获取这个锁，保证不再接收新的委托
}

var dummyBucket = &Bucket{ld: -1, rdx: -1, capacity: -1, segNum: -1}

// NewBucket 新建一个Bucket
func NewBucket(ld int, rdx int, capacity int, segNum int) *Bucket {
	return &Bucket{nil, ld, rdx, capacity, 0, segNum,
		sync.Map{}, sync.Map{}, sync.Map{}, time.Now().Unix(),
		make(map[string]util.KVPair), 0, make(map[string]map[string]int),
		sync.RWMutex{}, sync.RWMutex{}, sync.RWMutex{}, sync.Mutex{}}
}

// UpdateBucketToDB 更新bucket至db中
func (b *Bucket) UpdateBucketToDB(db *leveldb.DB, cache *[]interface{}) {
	//跳转到此函数时bucket已加写锁
	if cache != nil {
		targetCache, _ := (*cache)[1].(*lru.Cache[string, *Bucket])
		targetCache.Add("bucket"+util.IntArrayToString(b.BucketKey, b.rdx), b)
	} else {
		seBucket := SerializeBucket(b)
		if err := db.Put([]byte("bucket"+util.IntArrayToString(b.BucketKey, b.rdx)), seBucket, nil); err != nil {
			panic(err)
		}
	}
}

// NewSegment 新建一个segment
func NewSegment() []util.KVPair {
	kvPairs := make([]util.KVPair, 0)
	return kvPairs
}

func (b *Bucket) SetSegment(segKey string, db *leveldb.DB, cache *[]interface{}, value string, idx int) {
	//跳转到此函数时bucket已加写锁
	if _, ok := b.segments.Load(segKey); !ok {
		b.segLatch.Lock()
		if oldSeg, ok := b.segments.Load(segKey); ok {
			b.segLatch.Unlock()
			newSeg := oldSeg.([]util.KVPair)
			newSeg[idx].SetValue(value)
			b.segments.Store(segKey, newSeg)
			return
		}
		var kvs_ *[]util.KVPair
		key_ := util.IntArrayToString(b.BucketKey, b.rdx) + "segment" + segKey
		b.segIdxMaps.Delete(segKey)
		if cache != nil {
			targetCache, _ := (*cache)[2].(*lru.Cache[string, *[]util.KVPair])
			if kvs_, ok = targetCache.Get(key_); ok {
				b.segments.Store(segKey, *kvs_)
				toAddMap := sync.Map{}
				for i, kv := range *kvs_ {
					toAddMap.Store(kv.GetKey(), i)
				}
				b.segIdxMaps.Store(segKey, &toAddMap)
			}
		}
		if !ok {
			if kvString, error_ := db.Get([]byte(key_), nil); error_ == nil {
				kvs, segIdxMap, _ := DeserializeSegment(kvString)
				b.segments.Store(segKey, kvs)
				b.segIdxMaps.Store(segKey, segIdxMap)
			}
		}
		b.segLatch.Unlock()
	}
	if oldSeg, ok := b.segments.Load(segKey); ok {
		newSeg := oldSeg.([]util.KVPair)
		newSeg[idx].SetValue(value)
		b.segments.Store(segKey, newSeg)
	} else {
		fmt.Println("Error in SetSegment: Segment not found!")
	}
}

// GetSegment 获取segment,若不存在,则从db中获取
func (b *Bucket) GetSegment(segKey string, db *leveldb.DB, cache *[]interface{}) []util.KVPair {
	//跳转到此函数时bucket已加写锁
	if _, ok := b.segments.Load(segKey); !ok {
		b.segLatch.Lock()
		if ret, ok := b.segments.Load(segKey); ok {
			b.segLatch.Unlock()
			return ret.([]util.KVPair)
		}
		var kvs_ *[]util.KVPair
		key_ := util.IntArrayToString(b.BucketKey, b.rdx) + "segment" + segKey
		b.segIdxMaps.Delete(segKey)
		if cache != nil {
			targetCache, _ := (*cache)[2].(*lru.Cache[string, *[]util.KVPair])
			if kvs_, ok = targetCache.Get(key_); ok {
				b.segments.Store(segKey, *kvs_)
				toAddMap := sync.Map{}
				for idx, kv := range *kvs_ {
					toAddMap.Store(kv.GetKey(), idx)
				}
				b.segIdxMaps.Store(segKey, &toAddMap)
			}
		}
		if !ok {
			if kvString, error_ := db.Get([]byte(key_), nil); error_ == nil {
				kvs, segIdxMap, _ := DeserializeSegment(kvString)
				b.segments.Store(segKey, kvs)
				b.segIdxMaps.Store(segKey, segIdxMap)
			}
		}
		b.segLatch.Unlock()
	}
	if ret, ok := b.segments.Load(segKey); ok {
		return ret.([]util.KVPair)
	} else {
		return nil
	}
}

// UpdateSegmentToDB 更新segment至db中
func (b *Bucket) UpdateSegmentToDB(segKey string, db *leveldb.DB, cache *[]interface{}) {
	//跳转到此函数时桶已加写锁
	if _, ok := b.GetSegments().Load(segKey); !ok {
		return
	}
	if cache != nil {
		targetCache, _ := (*cache)[2].(*lru.Cache[string, *[]util.KVPair])
		v_, _ := b.segments.Load(segKey)
		v := v_.([]util.KVPair)
		targetCache.Add(util.IntArrayToString(b.BucketKey, b.rdx)+"segment"+segKey, &v)
	} else {
		v, _ := b.segments.Load(segKey)
		seSeg := SerializeSegment(v.([]util.KVPair))
		if err := db.Put([]byte(util.IntArrayToString(b.BucketKey, b.rdx)+"segment"+segKey), seSeg, nil); err != nil {
			panic(err)
		}
	}
}

// GetBucketKey returns the bucket key of the Bucket
func (b *Bucket) GetBucketKey() []int {
	return b.BucketKey
}

// GetLD returns the local depth of the Bucket
func (b *Bucket) GetLD() int {
	return b.ld
}

// GetRdx returns the rdx of the Bucket
func (b *Bucket) GetRdx() int {
	return b.rdx
}

// GetCapacity returns the capacity of the Bucket
func (b *Bucket) GetCapacity() int {
	return b.capacity
}

// GetNumber returns the number of data objects in the Bucket
func (b *Bucket) GetNumber() int {
	return b.number
}

// GetSegNum returns the number of segments in the Bucket
func (b *Bucket) GetSegNum() int {
	return b.segNum
}

// GetSegments returns the segments of the Bucket
func (b *Bucket) GetSegments() *sync.Map {
	return &b.segments
}

// GetMerkleTrees returns the merkle trees of the Bucket
func (b *Bucket) GetMerkleTrees() *sync.Map {
	return &b.merkleTrees
}

// GetMerkleTree 获取segment对应的merkle tree,若不存在,则从db中获取
func (b *Bucket) GetMerkleTree(index string, db *leveldb.DB, cache *[]interface{}) *mht.MerkleTree {
	//跳转到此函数时已对树加写锁或者对桶加写锁
	if v, ok := b.merkleTrees.Load(index); ok && v.(*mht.MerkleTree) == mht.DummyMerkleTree {
		b.mtLatch.Lock()
		if v, ok := b.merkleTrees.Load(index); ok && v.(*mht.MerkleTree) != mht.DummyMerkleTree {
			b.mtLatch.Unlock()
			return v.(*mht.MerkleTree)
		}
		var mt *mht.MerkleTree
		key_ := util.IntArrayToString(b.BucketKey, b.rdx) + "mht" + index
		if cache != nil {
			targetCache, _ := (*cache)[3].(*lru.Cache[string, *mht.MerkleTree])
			if mt, ok = targetCache.Get(key_); ok {
				b.merkleTrees.Store(index, mt)
			}
		}
		if !ok {
			if mtString, error_ := db.Get([]byte(key_), nil); error_ == nil {
				mt, _ = mht.DeserializeMHT(mtString)
				b.merkleTrees.Store(index, mt)
			}
		}
		b.mtLatch.Unlock()
	}
	if ret, ok := b.merkleTrees.Load(index); ok {
		return ret.(*mht.MerkleTree)
	} else {
		return nil
	}
}

// UpdateMerkleTreeToDB 将merkle tree更新至db中
func (b *Bucket) UpdateMerkleTreeToDB(index string, db *leveldb.DB, cache *[]interface{}) {
	//跳转到此函数时桶已加写锁
	if _, ok := b.GetMerkleTrees().Load(index); !ok {
		return
	}
	mt_, _ := b.GetMerkleTrees().Load(index)
	mt := mt_.(*mht.MerkleTree)
	if cache != nil {
		targetCache, _ := (*cache)[3].(*lru.Cache[string, *mht.MerkleTree])
		targetCache.Add(util.IntArrayToString(b.BucketKey, b.rdx)+"mht"+index, mt)
	} else {
		seMHT := mht.SerializeMHT(mt)
		if err := db.Put([]byte(util.IntArrayToString(b.BucketKey, b.rdx)+"mht"+index), seMHT, nil); err != nil {
			panic(err)
		}
	}
}

// SetBucketKey sets the bucket key of the Bucket
func (b *Bucket) SetBucketKey(bucketKey []int) {
	b.BucketKey = bucketKey
}

// SetLD sets the local depth of the Bucket
func (b *Bucket) SetLD(ld int) {
	b.ld = ld
}

// SetRdx sets the rdx of the Bucket
func (b *Bucket) SetRdx(rdx int) {
	b.rdx = rdx
}

// SetCapacity sets the capacity of the Bucket
func (b *Bucket) SetCapacity(capacity int) {
	b.capacity = capacity
}

// SetNumber sets the number of data objects in the Bucket
func (b *Bucket) SetNumber(number int) {
	b.number = number
}

// SetSegNum sets the number of segments in the Bucket
func (b *Bucket) SetSegNum(segNum int) {
	b.segNum = segNum
}

// GetSegmentKey 给定一个key，返回该key所在的segment map key
func (b *Bucket) GetSegmentKey(key string) string {
	if b.segNum < len(key) {
		return key[:b.segNum]
	} else {
		return ""
	}
}

// IsInBucket 给定一个key, 判断它是否在该bucket中
func (b *Bucket) IsInBucket(key string, db *leveldb.DB, cache *[]interface{}) bool {
	b.latch.RLock()
	defer b.latch.RUnlock()
	segKey := b.GetSegmentKey(key)
	b.GetSegment(segKey, db, cache) // 如果segments还没有从磁盘中读取，那么segIdxMaps[segKey]此时也会是缺省状态
	segIdxMap, _ := b.segIdxMaps.Load(segKey)
	if segIdxMap == nil {
		return false
	}
	_, ok := segIdxMap.(*sync.Map).Load(key)
	return ok
}

// GetValue 给定一个key, 返回它在该bucket中的value
func (b *Bucket) GetValue(key string, db *leveldb.DB, cache *[]interface{}) string {
	b.latch.RLock()
	defer b.latch.RUnlock()
	segKey := b.GetSegmentKey(key)
	b.GetSegment(segKey, db, cache) // 如果segments还没有从磁盘中读取，那么segIdxMaps[segKey]此时也会是缺省状态
	//判断是否在bucket中,在则返回value,不在则返回空字符串
	segIdxMap, _ := b.segIdxMaps.Load(segKey)
	if segIdxMap == nil {
		return ""
	}
	if idx, ok := segIdxMap.(*sync.Map).Load(key); ok {
		if segment, ok := b.segments.Load(segKey); ok {
			return segment.([]util.KVPair)[idx.(int)].GetValue()
		} else {
			return ""
		}
	}
	return ""
}

// GetIndex 给定一个key, 返回它所在的segKey，seg是否存在，在seg中的index, 如果不在, 返回-1
func (b *Bucket) GetIndex(key string, db *leveldb.DB, cache *[]interface{}) (string, bool, int) {
	//跳转到此函数是bucket已加锁
	segKey := b.GetSegmentKey(key)
	b.GetSegment(segKey, db, cache) // 如果segments还没有从磁盘中读取，那么segIdxMaps[segKey]此时也会是缺省状态
	segIdxMap, _ := b.segIdxMaps.Load(segKey)
	if segIdxMap == nil {
		return "", false, -1
	}
	if idx, ok := segIdxMap.(*sync.Map).Load(key); ok {
		return segKey, true, idx.(int)
	}
	return "", false, -1
}

// Insert 给定一个KVPair, 将它插入到该bucket中,返回插入后的bucket指针,若发生分裂,返回分裂后的rdx个bucket指针
func (b *Bucket) Insert(kvPair util.KVPair, db *leveldb.DB, cache *[]interface{}) [][]*Bucket {
	buckets := make([]*Bucket, 0)
	//判断是否在bucket中,在则返回所在的segment及其index,不在则返回-1
	segKey, _, index := b.GetIndex(kvPair.GetKey(), db, cache)
	if index != -1 {
		//在bucket中,修改value
		b.SetSegment(segKey, db, cache, kvPair.GetValue(), index)
		//将修改后的segment更新至db中
		b.UpdateSegmentToDB(segKey, db, cache)
		//更新bucket中对应segment的merkle tree
		b.GetMerkleTree(segKey, db, cache).UpdateRoot(index, []byte(kvPair.GetValue()))
		//将更新后的merkle tree更新至db中
		b.UpdateMerkleTreeToDB(segKey, db, cache)
		//返回更新后的bucket
		buckets = append(buckets, b)
		return [][]*Bucket{buckets}
	} else {
		//获得kvPair的key的segment
		segKey = b.GetSegmentKey(kvPair.GetKey())
		//判断bucket是否已满
		if b.number < b.capacity {
			//判断segment是否存在,不存在则创建,同时创建merkleTree
			if b.GetSegment(segKey, db, cache) == nil {
				b.segments.Store(segKey, NewSegment())
				b.segIdxMaps.Store(segKey, &sync.Map{})
			}
			//未满,插入到对应的segment中
			newSegment_, _ := b.segments.Load(segKey)
			newSegment := newSegment_.([]util.KVPair)
			newSegment = append(newSegment, kvPair)
			b.segments.Store(segKey, newSegment)
			//更新segment对应的segIdxMap
			segIdxMap_, _ := b.segIdxMaps.Load(segKey)
			segIdxMap := segIdxMap_.(*sync.Map)
			segIdxMap.Store(kvPair.GetKey(), len(newSegment)-1)
			b.segIdxMaps.Store(segKey, segIdxMap)
			//将更新后的segment更新至db中
			b.UpdateSegmentToDB(segKey, db, cache)
			b.number++
			//若该segment对应的merkle tree不存在,则创建,否则插入value到merkle tree中
			if b.GetMerkleTree(segKey, db, cache) == nil {
				b.merkleTrees.Store(segKey, mht.NewEmptyMerkleTree())
			}
			//插入value到merkle tree中,返回新的根哈希
			merkleTree_, _ := b.merkleTrees.Load(segKey)
			merkleTree := merkleTree_.(*mht.MerkleTree)
			merkleTree.InsertData([]byte(kvPair.GetValue()))
			b.merkleTrees.Store(segKey, merkleTree)
			//将更新后的merkle tree更新至db中
			b.UpdateMerkleTreeToDB(segKey, db, cache)
			buckets = append(buckets, b)
			return [][]*Bucket{buckets}
		} else {
			//已满,分裂成rdx个bucket
			ret := make([][]*Bucket, 0)
			buckets = append(buckets, b.SplitBucket(db, cache)...)
			//为所有bucket构建merkle tree
			for _, bucket := range buckets {
				segments := bucket.GetSegments()
				segments.Range(func(key, value interface{}) bool {
					//将kvPair转化为[][]byte
					data := make([][]byte, 0)
					for _, kvp := range value.([]util.KVPair) {
						data = append(data, []byte(kvp.GetValue()))
					}
					bucket.merkleTrees.Store(key.(string), mht.NewMerkleTree(data))
					//将merkleTrees[segKey]更新至db中
					bucket.UpdateMerkleTreeToDB(key.(string), db, cache)
					return true
				})
			}
			ret = append(ret, buckets)
			//判断key应该插入到哪个bucket中
			nextIdx := util.StringToBucketKeyIdxWithRdx(kvPair.GetKey(), b.ld, b.rdx)
			if ret_ := buckets[nextIdx].Insert(kvPair, db, cache); len(ret_[0]) != b.rdx { //说明这一次的插入没有引起桶分裂
				return ret
			} else { //此次插入引发桶分裂,分裂的rdx个桶作为新的一层桶加入到ret末尾
				return append(ret, ret_...)
			}
		}
	}
}

// SplitBucket 分裂bucket为rdx个bucket,并将原bucket中的数据重新分配到rdx个bucket中,返回rdx个bucket的指针
func (b *Bucket) SplitBucket(db *leveldb.DB, cache *[]interface{}) []*Bucket {
	buckets := make([]*Bucket, 0)
	b.SetLD(b.GetLD() + 1)
	originBKey := b.GetBucketKey()
	b.SetBucketKey(append([]int{0}, originBKey...))
	b.number = 0
	mMap := make(map[string][]util.KVPair)
	b.GetSegments().Range(func(key, value interface{}) bool {
		mMap[key.(string)] = value.([]util.KVPair)
		return true
	})
	b.segments = sync.Map{}
	b.segIdxMaps = sync.Map{}
	b.merkleTrees = sync.Map{}
	bToDelMap := b.toDelMap
	b.toDelMap = make(map[string]map[string]int)
	buckets = append(buckets, b)
	//创建rdx-1个新bucket
	for i := 0; i < b.rdx-1; i++ {
		newBucket := NewBucket(b.ld, b.rdx, b.capacity, b.segNum)
		newBucket.SetBucketKey(append([]int{i + 1}, originBKey...))
		//将新bucket插入到db中
		newBucket.UpdateBucketToDB(db, cache)
		buckets = append(buckets, newBucket)
	}
	//获取原bucket中所有数据对象
	for _, kvps := range mMap {
		for _, kvp := range kvps {
			//获取key的倒数第ld位
			//将数据对象插入到对应的bucket中
			buckets[util.StringToBucketKeyIdxWithRdx(kvp.GetKey(), b.ld, b.rdx)].Insert(kvp, db, cache)
		}
	}
	//将原bucket中待删除的数据对象插入到新的rdx个bucket中
	for key, value := range bToDelMap {
		buckets[util.StringToBucketKeyIdxWithRdx(key, b.ld, b.rdx)].toDelMap[key] = value
	}
	return buckets
}

// GetValueByKey 给定一个key, 返回它的value,所在segKey,是否存在,在seg中的index, 如果不在, 返回-1
func (b *Bucket) GetValueByKey(key string, db *leveldb.DB, cache *[]interface{}, isBucketLockFree bool) (string, string, bool, int) {
	if !isBucketLockFree {
		b.latch.RLock()
		defer b.latch.RUnlock()
	}
	segKey, isSegExist, index := b.GetIndex(key, db, cache)
	if index == -1 {
		return "", segKey, isSegExist, index
	}
	//value := b.GetSegment(segKey, db, cache)[index].GetValue()
	//TODO Debug
	seg := b.GetSegment(segKey, db, cache)
	if index < len(seg) {
		return seg[index].GetValue(), segKey, isSegExist, index
	} else {
		return "", segKey, isSegExist, index
	}
}

// GetProof 给定一个key, 返回它所在segment的根哈希,存在的proof；若key不存在，判断segment是否存在：若存在，则返回此seg中所有值及其哈希，否则返回所有segKey及其segRootHash
func (b *Bucket) GetProof(segKey string, isSegExist bool, index int, db *leveldb.DB, cache *[]interface{}) ([32]byte, *mht.MHTProof) {
	b.latch.RLock()
	defer b.latch.RUnlock()
	segHash := b.GetMerkleTree(segKey, db, cache) // 为bucket加读锁后树最多更新一次
	if index != -1 {
		return segHash.GetRootHash(), segHash.GetProof(index)
	} else if isSegExist {
		//segment存在,但key不存在,返回此seg中所有值及其哈希
		kvPairs := b.GetSegment(segKey, db, cache)
		values := make([]string, len(kvPairs))
		for i := 0; i < len(kvPairs); i++ {
			values[i] = kvPairs[i].GetValue()
		}
		return segHash.GetRootHash(), mht.NewMHTProof(false, nil, true, values, nil, nil)
	} else {
		//segment不存在,返回所有segKey及其segRootHash
		segKeys := make([]string, 0)
		segRootHashes := make([][32]byte, 0)
		b.merkleTrees.Range(func(key, value interface{}) bool {
			segKeys = append(segKeys, key.(string))
			segRootHashes = append(segRootHashes, b.GetMerkleTree(key.(string), db, cache).GetRootHash())
			return true
		})
		return [32]byte{}, mht.NewMHTProof(false, nil, false, nil, segKeys, segRootHashes)
	}
}

// ComputeSegHashRoot 给定value和segProof，返回由它们计算得到的segHashRoot
func ComputeSegHashRoot(value string, proofPairs []mht.ProofPair) [32]byte {
	fmt.Printf("value: %s\n", value)
	fmt.Printf("value byte:%x\n", []byte(value))
	//对value求hash
	data := sha256.Sum256([]byte(value))
	//逐层计算segHashRoot
	for i := 0; i < len(proofPairs); i++ {
		parentHash := make([]byte, 0)
		if proofPairs[i].Index == 0 {
			parentHash = append(proofPairs[i].Hash[:], data[:]...)
		} else {
			fmt.Printf("data: %x\n", data)
			fmt.Printf("proofHash:%x\n", proofPairs[i].Hash)
			parentHash = append(data[:], proofPairs[i].Hash[:]...)
		}
		data = sha256.Sum256(parentHash)
	}
	return data
}

// PrintBucket 打印bucket中的所有数据对象
func (b *Bucket) PrintBucket(db *leveldb.DB, cache *[]interface{}) {
	//打印ld,rdx,capacity,number,segNum
	fmt.Printf("bucketKey:%s,ld: %d,rdx: %d,capacity: %d,number: %d,segNum: %d\n", util.IntArrayToString(b.BucketKey, b.rdx), b.ld, b.rdx, b.capacity, b.number, b.segNum)
	//打印segments
	b.latch.RLock()
	defer b.latch.RUnlock()
	b.GetSegments().Range(func(key, value interface{}) bool {
		fmt.Printf("segKey: %s\n", key.(string))
		for _, kvp := range value.([]util.KVPair) {
			fmt.Printf("%s:%s\n", kvp.GetKey(), kvp.GetValue())
		}
		return true
	})
	//打印所有merkle tree
	b.GetMerkleTrees().Range(func(key, value interface{}) bool {
		fmt.Printf("Merkle Tree of Segement %s:\n", key.(string))
		b.GetMerkleTree(key.(string), db, cache).PrintTree()
		return true
	})
}

// PrintMHTProof 打印mhtProof mht.MHTProof
func PrintMHTProof(mhtProof *mht.MHTProof) {
	if mhtProof.GetIsExist() {
		fmt.Printf("KV存在证明:\n")
		proofPairs := mhtProof.GetProofPairs()
		for i := 0; i < len(proofPairs); i++ {
			fmt.Printf("[%d, %x]\n", proofPairs[i].Index, proofPairs[i].Hash)
		}
	} else if mhtProof.GetIsSegExist() {
		fmt.Printf("KV不存在,但segment存在,segment中所有的值:\n")
		values := mhtProof.GetValues()
		for i := 0; i < len(values); i++ {
			fmt.Printf("%s\n", values[i])
		}
	} else {
		fmt.Printf("KV不存在,segment也不存在,所有segment的segkey和segRootHash:\n")
		segKeys := mhtProof.GetSegKeys()
		segRootHashes := mhtProof.GetSegRootHashes()
		for i := 0; i < len(segKeys); i++ {
			fmt.Printf("segKey: %s, segRootHash: %x\n", segKeys[i], segRootHashes[i])
		}
	}
}

type SeSegment struct {
	KVPairs []util.SeKVPair // data objects in each segment
}

func SerializeSegment(kvPairs []util.KVPair) []byte {
	seKvP := make([]util.SeKVPair, 0)
	for _, kvp := range kvPairs {
		seKvP = append(seKvP, util.SeKVPair{Key: kvp.GetKey(), Value: kvp.GetValue()})
	}
	seSegment := &SeSegment{seKvP}
	jsonSegment, err := json.Marshal(seSegment)
	if err != nil {
		fmt.Printf("SerializeSegment error: %v\n", err)
		return nil
	}
	return jsonSegment
}

func DeserializeSegment(data []byte) ([]util.KVPair, *sync.Map, error) {
	var seSegment SeSegment
	err := json.Unmarshal(data, &seSegment)
	if err != nil {
		fmt.Printf("DeserializeSegment error: %v\n", err)
		return nil, &sync.Map{}, err
	}
	kvPairs := make([]util.KVPair, 0)
	segIdxMap := sync.Map{}
	for idx, seKvP := range seSegment.KVPairs {
		key, val := seKvP.GetKey(), seKvP.GetValue()
		kvPairs = append(kvPairs, *util.NewKVPair(key, val))
		segIdxMap.Store(key, idx)
	}
	return kvPairs, &segIdxMap, nil
}

type SeBucket struct {
	BucketKey []int    // bucket key, initial nil, related to ld and kvPair.key
	Ld        int      // local depth, initial zero
	Rdx       int      // rdx, initial 2
	Capacity  int      // capacity of the bucket, initial given
	Number    int      // number of data objects in the bucket, initial zero
	SegNum    int      // number of segment bits in the bucket, initial given
	SegKeys   []string // segment keys of segments, is used to index the segments and mHts in leveldb
}

// SerializeBucket 序列化Bucket
func SerializeBucket(b *Bucket) []byte {
	//跳转到此函数时bucket已加锁或者没有后续插入
	seBucket := &SeBucket{b.BucketKey, b.ld, b.rdx, b.capacity, b.number, b.segNum, make([]string, 0)}
	b.segments.Range(func(key, value interface{}) bool {
		seBucket.SegKeys = append(seBucket.SegKeys, key.(string))
		return true
	})
	jsonBucket, err := json.Marshal(seBucket)
	if err != nil {
		fmt.Printf("SerializeBucket error: %v\n", err)
		return nil
	}
	return jsonBucket
}

// DeserializeBucket 反序列化Bucket
func DeserializeBucket(data []byte) (*Bucket, error) {
	var seBucket SeBucket
	err := json.Unmarshal(data, &seBucket)
	if err != nil {
		fmt.Printf("DeserializeBucket error: %v\n", err)
		return nil, err
	}
	bucket := &Bucket{seBucket.BucketKey, seBucket.Ld, seBucket.Rdx, seBucket.Capacity, seBucket.Number,
		seBucket.SegNum, sync.Map{}, sync.Map{}, sync.Map{}, time.Now().Unix(),
		make(map[string]util.KVPair), 0, make(map[string]map[string]int), sync.RWMutex{}, sync.RWMutex{}, sync.RWMutex{}, sync.Mutex{}}
	for i := 0; i < len(seBucket.SegKeys); i++ {
		bucket.merkleTrees.Store(seBucket.SegKeys[i], mht.DummyMerkleTree)
	}
	return bucket, nil
}

type BucketDelegationCode int

const (
	CLIENT = iota
	DELEGATE
	FAILED
)
