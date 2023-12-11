package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"strings"
	"sync"
)

//NewBucket(ld int, rdx int, capacity int, segNum int) *Bucket {}: creates a new Bucket object
//GetSegment(key string) string {} :给定一个key，返回该key所在的segment map key
//IsInBucket(key string) bool {}:给定一个key, 判断它是否在该bucket中
//GetValue(key string) string {}:给定一个key, 返回它在该bucket中的value
//GetIndex(key string) (string, int) {}:给定一个key, 返回它所在的segment及其index, 如果不在, 返回-1
//Insert(kvpair util.KVPair) []*Bucket {}: 给定一个KVPair, 将它插入到该bucket中,返回插入后的bucket指针,若发生分裂,返回分裂后的rdx个bucket指针
//GetProof(key string) (string, []byte, []mht.ProofPair) {}: 给定一个key, 返回它的value,所在segment的根哈希,存在的proof,若不存在,返回空字符串,空字符串,空数组
//PrintBucket() {}: 打印bucket中的所有数据对象
//PrintMHTProof(proof []mht.ProofPair) {}: 打印Proof []ProofPair
//ComputSegHashRoot(value string, segProof []mht.ProofPair) []byte {}: 给定value和segProof，返回由它们计算得到的segHashRoot

type Bucket struct {
	BucketKey []int // bucket key, initial nil, related to ld and kvpair.key

	ld  int // local depth, initial zero
	rdx int // rdx, initial 2

	capacity int // capacity of the bucket, initial given
	number   int // number of data objects in the bucket, initial zero

	segNum      int                      // number of segment bits in the bucket, initial given
	segments    map[string][]util.KVPair // segments: data objects in each segment
	segIdxMaps  map[string]map[string]int
	merkleTrees map[string]*mht.MerkleTree // merkle trees: one for each segment

	latchTimestamp    int64
	DelegationList    map[string]util.KVPair // 委托插入的数据，用于后续一并插入，使用map结构是因为委托插入的数据可能键相同，需要通过key去找到并合并,map的key就是KVPair的key
	RootLatchGainFlag bool                   // 用于判断被委托线程是否已经获取到树根，如果获取到了，那么置为True，其余线程停止对DelegationLatch进行获取尝试，保证被委托线程在获取到树根以后可以尽快获取到DelegationLatch并开始不接受委托
	latch             sync.RWMutex

	segLatch        sync.RWMutex
	mtLatch         sync.RWMutex
	DelegationLatch sync.Mutex // 委托线程锁，用于将待插入数据更新到委托插入数据集，当被委托线程获取到MGT树根写锁时，也会试图获取这个锁，保证不再接收新的委托
}

var dummyBucket = &Bucket{ld: -1, rdx: -1, capacity: -1, segNum: -1}

// 新建一个Bucket
func NewBucket(ld int, rdx int, capacity int, segNum int) *Bucket {
	return &Bucket{nil, ld, rdx, capacity, 0, segNum,
		make(map[string][]util.KVPair), make(map[string]map[string]int), make(map[string]*mht.MerkleTree), 0,
		make(map[string]util.KVPair), false, sync.RWMutex{}, sync.RWMutex{},
		sync.RWMutex{}, sync.Mutex{}}
}

// 更新bucket至db中
func (b *Bucket) UpdateBucketToDB(db *leveldb.DB, cache *[]interface{}) {
	//跳转到此函数时bucket已加写锁
	if cache != nil {
		targetCache, _ := (*cache)[1].(*lru.Cache[string, *Bucket])
		targetCache.Add("bucket"+util.IntArrayToString(b.BucketKey, b.rdx), b)
	} else {
		seBucket := SerializeBucket(b)
		//fmt.Printf("write bucket %s to DB.\n", b.name+"bucket"+util.IntArrayToString(b.BucketKey, b.rdx))
		if err := db.Put([]byte("bucket"+util.IntArrayToString(b.BucketKey, b.rdx)), seBucket, nil); err != nil {
			panic(err)
		}
	}
}

// 新建一个segment
func NewSegment() []util.KVPair {
	kvPairs := make([]util.KVPair, 0)
	return kvPairs
}

// 获取segment,若不存在,则从db中获取
func (b *Bucket) GetSegment(segkey string, db *leveldb.DB, cache *[]interface{}) []util.KVPair {
	//跳转到此函数时bucket已加写锁
	if len(b.segments[segkey]) == 0 {
		//fmt.Printf("read segment %s from DB.\n", b.name+util.IntArrayToString(b.bucketKey)+"segment"+segkey)
		var ok bool
		var kvs_ *[]util.KVPair
		key_ := util.IntArrayToString(b.BucketKey, b.rdx) + "segment" + segkey
		b.segIdxMaps[segkey] = nil
		if cache != nil {
			targetCache, _ := (*cache)[2].(*lru.Cache[string, *[]util.KVPair])
			if kvs_, ok = targetCache.Get(key_); ok {
				b.segments[segkey] = *kvs_
				b.segIdxMaps[segkey] = make(map[string]int)
				for idx, seg := range b.segments[segkey] {
					b.segIdxMaps[segkey][seg.GetKey()] = idx
				}
			}
		}
		if !ok {
			if kvString, error_ := db.Get([]byte(key_), nil); error_ == nil {
				kvs, segIdxMap, _ := DeserializeSegment(kvString)
				b.segments[segkey] = kvs
				b.segIdxMaps[segkey] = segIdxMap
			}
		}
	}
	return b.segments[segkey]
}

// 更新segment至db中
func (b *Bucket) UpdateSegmentToDB(segkey string, db *leveldb.DB, cache *[]interface{}) {
	//跳转到此函数时桶已加写锁
	if b.GetSegments()[segkey] == nil {
		return
	}
	if cache != nil {
		targetCache, _ := (*cache)[2].(*lru.Cache[string, *[]util.KVPair])
		v := b.segments[segkey]
		targetCache.Add(util.IntArrayToString(b.BucketKey, b.rdx)+"segment"+segkey, &v)
	} else {
		seSeg := SerializeSegment(b.segments[segkey])
		//fmt.Printf("write segment %s to DB.\n", b.name+util.IntArrayToString(b.bucketKey)+"segment"+segkey)
		if err := db.Put([]byte(util.IntArrayToString(b.BucketKey, b.rdx)+"segment"+segkey), seSeg, nil); err != nil {
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
func (b *Bucket) GetSegments() map[string][]util.KVPair {
	return b.segments
}

// GetMerkleTrees returns the merkle trees of the Bucket
func (b *Bucket) GetMerkleTrees() map[string]*mht.MerkleTree {
	return b.merkleTrees
}

// 获取segment对应的merkle tree,若不存在,则从db中获取
func (b *Bucket) GetMerkleTree(index string, db *leveldb.DB, cache *[]interface{}) *mht.MerkleTree {
	//跳转到此函数时已对树加写锁或者对桶加写锁
	if b.merkleTrees[index] == nil {
		//fmt.Printf("read mht %s to DB.\n", b.name+util.IntArrayToString(b.bucketKey, b.rdx)+"mht"+index)
		var ok bool
		var mt *mht.MerkleTree
		key_ := util.IntArrayToString(b.BucketKey, b.rdx) + "mht" + index
		if cache != nil {
			targetCache, _ := (*cache)[3].(*lru.Cache[string, *mht.MerkleTree])
			if mt, ok = targetCache.Get(key_); ok {
				b.merkleTrees[index] = mt
			}
		}
		if !ok {
			if mtString, error_ := db.Get([]byte(key_), nil); error_ == nil {
				mt, _ = mht.DeserializeMHT(mtString)
				b.merkleTrees[index] = mt
			}
		}
	}
	return b.merkleTrees[index]
}

// 将merkle tree更新至db中
func (b *Bucket) UpdateMerkleTreeToDB(index string, db *leveldb.DB, cache *[]interface{}) {
	//跳转到此函数时桶已加写锁
	if b.GetMerkleTrees()[index] == nil {
		return
	}
	mt := b.GetMerkleTrees()[index]
	if cache != nil {
		targetCache, _ := (*cache)[3].(*lru.Cache[string, *mht.MerkleTree])
		targetCache.Add(util.IntArrayToString(b.BucketKey, b.rdx)+"mht"+index, mt)
	} else {
		seMHT := mht.SerializeMHT(mt)
		//fmt.Printf("write mht %s to DB.\n", b.name+util.IntArrayToString(b.bucketKey)+"mht"+index)
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

// 给定一个key，返回该key所在的segment map key
func (b *Bucket) GetSegmentKey(key string) string {
	if b.segNum < len(key) {
		return key[:b.segNum]
	} else {
		return ""
	}
}

// 给定一个key, 判断它是否在该bucket中
func (b *Bucket) IsInBucket(key string, db *leveldb.DB, cache *[]interface{}) bool {
	b.latch.RLock()
	defer b.latch.RUnlock()
	segkey := b.GetSegmentKey(key)
	b.segLatch.Lock()
	defer b.segLatch.Unlock()       // 防止segIdxMaps并发读写，segIdxMaps只会在GetSegment调用时可能被修改，上锁后保证只有一个线程对其读或写
	b.GetSegment(segkey, db, cache) // 如果segments还没有从磁盘中读取，那么segIdxMaps[segkey]此时也会是缺省状态
	_, ok := b.segIdxMaps[segkey][key]
	return ok
}

// 给定一个key, 返回它在该bucket中的value
func (b *Bucket) GetValue(key string, db *leveldb.DB, cache *[]interface{}) string {
	b.latch.RLock()
	defer b.latch.RUnlock()
	segkey := b.GetSegmentKey(key)
	b.segLatch.Lock()
	defer b.segLatch.Unlock()       // 防止segIdxMaps并发读写，segIdxMaps只会在GetSegment调用时可能被修改，上锁后保证只有一个线程对其读或写
	b.GetSegment(segkey, db, cache) // 如果segments还没有从磁盘中读取，那么segIdxMaps[segkey]此时也会是缺省状态
	//判断是否在bucket中,在则返回value,不在则返回空字符串
	if idx, ok := b.segIdxMaps[segkey][key]; ok {
		return b.segments[segkey][idx].GetValue()
	}
	return ""
}

// 给定一个key, 返回它所在的segkey，seg是否存在，在seg中的index, 如果不在, 返回-1
func (b *Bucket) GetIndex(key string, db *leveldb.DB, cache *[]interface{}) (string, bool, int) {
	//跳转到此函数是bucket已加锁
	segkey := b.GetSegmentKey(key)
	b.segLatch.Lock()
	defer b.segLatch.Unlock()       // 防止segIdxMaps并发读写，segIdxMaps只会在GetSegment调用时可能被修改，上锁后保证只有一个线程对其读或写
	b.GetSegment(segkey, db, cache) // 如果segments还没有从磁盘中读取，那么segIdxMaps[segkey]此时也会是缺省状态
	if idx, ok := b.segIdxMaps[segkey][key]; ok {
		return segkey, true, idx
	}
	return "", false, -1
}

// 给定一个KVPair, 将它插入到该bucket中,返回插入后的bucket指针,若发生分裂,返回分裂后的rdx个bucket指针
func (b *Bucket) Insert(kvpair util.KVPair, db *leveldb.DB, cache *[]interface{}) [][]*Bucket {
	buckets := make([]*Bucket, 0)
	//判断是否在bucket中,在则返回所在的segment及其index,不在则返回-1
	segkey, _, index := b.GetIndex(kvpair.GetKey(), db, cache)
	if index != -1 {
		//在bucket中,修改value
		b.GetSegment(segkey, db, cache)[index].SetValue(kvpair.GetValue())
		//将修改后的segment更新至db中
		b.UpdateSegmentToDB(segkey, db, cache)
		//更新bucket中对应segment的merkle tree
		b.GetMerkleTree(segkey, db, cache).UpdateRoot(index, []byte(kvpair.GetValue()))
		//将更新后的merkle tree更新至db中
		b.UpdateMerkleTreeToDB(segkey, db, cache)
		//返回更新后的bucket
		buckets = append(buckets, b)
		return [][]*Bucket{buckets}
	} else {
		//获得kvpair的key的segment
		segkey := b.GetSegmentKey(kvpair.GetKey())
		//判断bucket是否已满
		if b.number < b.capacity {
			//判断segment是否存在,不存在则创建,同时创建merkleTree
			if b.GetSegment(segkey, db, cache) == nil {
				b.segments[segkey] = NewSegment()
				b.segIdxMaps[segkey] = make(map[string]int)
			}
			//未满,插入到对应的segment中
			b.segments[segkey] = append(b.segments[segkey], kvpair)
			b.segIdxMaps[segkey][kvpair.GetKey()] = len(b.segments[segkey]) - 1
			//将更新后的segment更新至db中
			b.UpdateSegmentToDB(segkey, db, cache)
			b.number++
			//若该segment对应的merkle tree不存在,则创建,否则插入value到merkle tree中
			if b.GetMerkleTree(segkey, db, cache) == nil {
				b.merkleTrees[segkey] = mht.NewEmptyMerkleTree()
			}
			//插入value到merkle tree中,返回新的根哈希
			b.merkleTrees[segkey].InsertData([]byte(kvpair.GetValue()))
			//将更新后的merkle tree更新至db中
			b.UpdateMerkleTreeToDB(segkey, db, cache)
			buckets = append(buckets, b)
			return [][]*Bucket{buckets}
		} else {
			//已满,分裂成rdx个bucket
			//fmt.Printf("bucket(%s)已满,分裂成%d个bucket\n", util.IntArrayToString(b.GetBucketKey()), b.rdx)
			ret := make([][]*Bucket, 0)
			buckets = append(buckets, b.SplitBucket(db, cache)...)
			//为所有bucket构建merkle tree
			for _, bucket := range buckets {
				segments := bucket.GetSegments()
				for segkey, kvpairs := range segments {
					//将kvpair转化为[][]byte
					datas := make([][]byte, 0)
					for _, kvpair := range kvpairs {
						datas = append(datas, []byte(kvpair.GetValue()))
					}
					bucket.merkleTrees[segkey] = mht.NewMerkleTree(datas)
					//将merkleTrees[segkey]更新至db中
					bucket.UpdateMerkleTreeToDB(segkey, db, cache)
				}
			}
			//判断key应该插入到哪个bucket中
			//fmt.Printf("已分裂成%d个bucket,key=%s应该插入到第%d个bucket中\n", b.rdx, ikey, bk)
			ret = append(ret, buckets)
			if ret_ := buckets[util.StringToBucketKeyIdxWithRdx(kvpair.GetKey(), b.ld, b.rdx)].Insert(kvpair, db, cache); len(ret_[0]) != b.rdx { //说明这一次的插入没有引起桶分裂
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
	originBkey := b.GetBucketKey()
	b.SetBucketKey(append([]int{0}, originBkey...))
	b.number = 0
	bsegments := b.GetSegments()
	b.segments = nil
	b.segments = make(map[string][]util.KVPair)
	b.merkleTrees = nil
	b.merkleTrees = make(map[string]*mht.MerkleTree)
	b.segIdxMaps = nil
	b.segIdxMaps = make(map[string]map[string]int)
	buckets = append(buckets, b)
	//创建rdx-1个新bucket
	for i := 0; i < b.rdx-1; i++ {
		newBucket := NewBucket(b.ld, b.rdx, b.capacity, b.segNum)
		newBucket.SetBucketKey(append([]int{i + 1}, originBkey...))
		//将新bucket插入到db中
		newBucket.UpdateBucketToDB(db, cache)
		buckets = append(buckets, newBucket)
	}
	//获取原bucket中所有数据对象
	for _, value := range bsegments {
		kvpairs := value
		for _, kvpair := range kvpairs {
			//获取key的倒数第ld位
			//将数据对象插入到对应的bucket中
			buckets[util.StringToBucketKeyIdxWithRdx(kvpair.GetKey(), b.ld, b.rdx)].Insert(kvpair, db, cache)
		}
	}
	return buckets
}

// 给定一个key, 返回它的value,所在segkey,是否存在,在seg中的index, 如果不在, 返回-1
func (b *Bucket) GetValueByKey(key string, db *leveldb.DB, cache *[]interface{}, isBucketLockFree bool) (string, string, bool, int) {
	if !isBucketLockFree {
		b.latch.RLock()
		defer b.latch.RUnlock()
	}
	segkey, isSegExist, index := b.GetIndex(key, db, cache)
	if index == -1 {
		return "", segkey, isSegExist, index
	}
	b.segLatch.Lock()
	value := b.GetSegment(segkey, db, cache)[index].GetValue()
	b.segLatch.Unlock()
	return value, segkey, isSegExist, index
}

// 给定一个key, 返回它所在segment的根哈希,存在的proof；若key不存在，判断segment是否存在：若存在，则返回此seg中所有值及其哈希，否则返回所有segkey及其segRootHash
func (b *Bucket) GetProof(segkey string, isSegExist bool, index int, db *leveldb.DB, cache *[]interface{}) ([]byte, *mht.MHTProof) {
	b.latch.RLock()
	defer b.latch.RUnlock()
	b.mtLatch.Lock()
	segHash := b.GetMerkleTree(segkey, db, cache) // 为bucket加读锁后树最多更新一次
	b.mtLatch.Unlock()
	if index != -1 {
		segHashRoot := segHash.GetRootHash()
		proof := segHash.GetProof(index)
		return segHashRoot, proof
	} else if isSegExist {
		//segment存在,但key不存在,返回此seg中所有值及其哈希
		b.segLatch.Lock()
		kvpairs := b.GetSegment(segkey, db, cache)
		b.segLatch.Unlock()
		values := make([]string, 0)
		for i := 0; i < len(kvpairs); i++ {
			values = append(values, kvpairs[i].GetValue())
		}
		segHashRoot := segHash.GetRootHash()
		return segHashRoot, mht.NewMHTProof(false, nil, true, values, nil, nil)
	} else {
		//segment不存在,返回所有segkey及其segRootHash
		segKeys := make([]string, 0)
		segRootHashes := make([][]byte, 0)
		for segkey := range b.merkleTrees {
			segKeys = append(segKeys, segkey)
			b.mtLatch.Lock()
			segHash := b.GetMerkleTree(segkey, db, cache)
			b.mtLatch.Unlock()
			segRootHashes = append(segRootHashes, segHash.GetRootHash())
		}
		return nil, mht.NewMHTProof(false, nil, false, nil, segKeys, segRootHashes)
	}
}

// 给定value和segProof，返回由它们计算得到的segHashRoot
func ComputSegHashRoot(value string, proofPairs []mht.ProofPair) []byte {
	fmt.Printf("value: %s\n", value)
	fmt.Printf("value byte:%x\n", []byte(value))
	//对value求hash
	valuehash := sha256.Sum256([]byte(value))
	data := valuehash[:]
	//逐层计算segHashRoot
	for i := 0; i < len(proofPairs); i++ {
		parentHash := make([]byte, 0)
		if proofPairs[i].Index == 0 {
			parentHash = append(proofPairs[i].Hash, data...)
		} else {
			fmt.Printf("data: %x\n", data)
			fmt.Printf("proofHash:%x\n", proofPairs[i].Hash)
			parentHash = append(data, proofPairs[i].Hash...)
		}
		hash := sha256.Sum256(parentHash)
		data = hash[:]
	}
	return data
}

// 打印bucket中的所有数据对象
func (b *Bucket) PrintBucket(db *leveldb.DB, cache *[]interface{}) {
	//打印ld,rdx,capacity,number,segNum
	fmt.Printf("bucketKey:%s,ld: %d,rdx: %d,capacity: %d,number: %d,segNum: %d\n", util.IntArrayToString(b.BucketKey, b.rdx), b.ld, b.rdx, b.capacity, b.number, b.segNum)
	//打印segments
	b.latch.RLock()
	defer b.latch.RUnlock()
	b.segLatch.Lock()
	for segkey := range b.GetSegments() {
		fmt.Printf("segkey: %s\n", segkey)
		kvpairs := b.GetSegment(segkey, db, cache)
		for _, kvpair := range kvpairs {
			fmt.Printf("%s:%s\n", kvpair.GetKey(), kvpair.GetValue())
		}
	}
	b.segLatch.Unlock()
	//打印所有merkle tree
	b.mtLatch.Lock()
	for segkey := range b.GetMerkleTrees() {
		fmt.Printf("Merkle Tree of Segement %s:\n", segkey)
		b.GetMerkleTree(segkey, db, cache).PrintTree()
	}
	b.mtLatch.Unlock()
}

// 打印mhtProof mht.MHTProof
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
		segkeys := mhtProof.GetSegKeys()
		segRootHashes := mhtProof.GetSegRootHashes()
		for i := 0; i < len(segkeys); i++ {
			fmt.Printf("segKey: %s, segRootHash: %x\n", segkeys[i], segRootHashes[i])
		}
	}
}

type SeSegment struct {
	KVPairs string // data objects in each segment
}

func SerializeSegment(kvpairs []util.KVPair) []byte {
	kvstring := ""
	for i := 0; i < len(kvpairs); i++ {
		kvstring += kvpairs[i].GetKey() + ":" + kvpairs[i].GetValue()
		if i < len(kvpairs)-1 {
			kvstring += ";"
		}
	}
	seSegment := &SeSegment{kvstring}
	jsonSegment, err := json.Marshal(seSegment)
	if err != nil {
		fmt.Printf("SerializeSegment error: %v\n", err)
		return nil
	}
	return jsonSegment
}

func DeserializeSegment(data []byte) ([]util.KVPair, map[string]int, error) {
	var seSegment SeSegment
	err := json.Unmarshal(data, &seSegment)
	if err != nil {
		fmt.Printf("DeserializeSegment error: %v\n", err)
		return nil, nil, err
	}
	kvstrings := strings.Split(seSegment.KVPairs, ";")
	kvpairs := make([]util.KVPair, 0)
	segIdxMap := make(map[string]int)
	idx := 0
	for i := 0; i < len(kvstrings); i++ {
		kvpair := strings.Split(kvstrings[i], ":")
		if len(kvpair) == 2 {
			kvpairs = append(kvpairs, *util.NewKVPair(kvpair[0], kvpair[1]))
			segIdxMap[kvpair[0]] = idx
			idx++
		}
	}
	return kvpairs, segIdxMap, nil
}

type SeBucket struct {
	BucketKey []int // bucket key, initial nil, related to ld and kvpair.key

	Ld  int // local depth, initial zero
	Rdx int // rdx, initial 2

	Capacity int // capacity of the bucket, initial given
	Number   int // number of data objects in the bucket, initial zero

	SegNum  int      // number of segment bits in the bucket, initial given
	SegKeys []string // segment keys of segments, is used to index the segments and mhts in leveldb
}

// 序列化Bucket
func SerializeBucket(b *Bucket) []byte {
	//跳转到此函数时bucket已加锁或者没有后续插入
	seBucket := &SeBucket{b.BucketKey, b.ld, b.rdx, b.capacity, b.number, b.segNum, make([]string, 0)}
	for k := range b.segments {
		seBucket.SegKeys = append(seBucket.SegKeys, k)
	}
	jsonBucket, err := json.Marshal(seBucket)
	if err != nil {
		fmt.Printf("SerializeBucket error: %v\n", err)
		return nil
	}
	return jsonBucket
}

// 反序列化Bucket
func DeserializeBucket(data []byte) (*Bucket, error) {
	var seBucket SeBucket
	err := json.Unmarshal(data, &seBucket)
	if err != nil {
		fmt.Printf("DeserializeBucket error: %v\n", err)
		return nil, err
	}
	segments := make(map[string][]util.KVPair)
	mhts := make(map[string]*mht.MerkleTree)
	for i := 0; i < len(seBucket.SegKeys); i++ {
		segments[seBucket.SegKeys[i]] = nil
		mhts[seBucket.SegKeys[i]] = nil
	}
	bucket := &Bucket{seBucket.BucketKey, seBucket.Ld, seBucket.Rdx, seBucket.Capacity, seBucket.Number,
		seBucket.SegNum, segments, make(map[string]map[string]int), mhts, 0,
		make(map[string]util.KVPair), false, sync.RWMutex{}, sync.RWMutex{},
		sync.RWMutex{}, sync.Mutex{}}
	return bucket, nil
}

type bucketDelegationCode int

const (
	CLIENT = iota
	DELEGATE
	FAILED
)
