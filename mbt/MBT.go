package mbt

import (
	"MEHT/util"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/emirpasic/gods/queues/arrayqueue"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type MBT struct {
	bucketNum   int
	aggregation int
	gd          int
	mbtHash     []byte
	rootHash    []byte
	Root        *MBTNode
	cache       *lru.Cache[string, *MBTNode]
	cacheEnable bool
	latch       sync.RWMutex
	updateLatch sync.Mutex
}

func NewMBT(bucketNum int, aggregation int, db *leveldb.DB, cacheEnable bool, mbtNodeCC int) *MBT {
	//此处就应该将全部的结构都初始化一遍
	var root *MBTNode
	var c *lru.Cache[string, *MBTNode]
	gd := 0
	if cacheEnable {
		c, _ = lru.NewWithEvict[string, *MBTNode](mbtNodeCC, func(k string, v *MBTNode) {
			callBackFoo[string, *MBTNode](k, v, db)
		})
	}
	if bucketNum <= 0 {
		panic("BucketNum of MBT must exceed 0.")
	} else if bucketNum == 1 {
		gd++
		root = NewMBTNode([]byte("Root"), nil, make([][]byte, 1), true, db, c)
	} else {
		queue := arrayqueue.New()
		for i := 0; i < bucketNum; i++ {
			queue.Enqueue(NewMBTNode([]byte("LeafNode"+strconv.Itoa(i)), nil, make([][]byte, 1), true, db, c))
		}
		offset := 0
		for !queue.Empty() {
			gd++
			s := queue.Size()
			if s == 1 {
				root_, _ := queue.Dequeue()
				root = root_.(*MBTNode)
				oldHash := root.nodeHash
				root.nodeHash = []byte("Root")
				root.name = root.nodeHash
				if c != nil {
					c.Remove(string(oldHash))
					c.Add(string(root.nodeHash), root)
				}
				if err := db.Delete(oldHash, nil); err != nil {
					fmt.Println("Error in NewMBT: ", err)
				}
				break
			}
			parSize := s / aggregation
			if s%aggregation != 0 {
				parSize++
			}
			for i := 0; i < parSize; i++ {
				sSubNodes := make([]*MBTNode, 0)
				dDataHashes := make([][]byte, 0)
				for j := 0; j < aggregation && !queue.Empty(); j++ {
					cNode_, _ := queue.Dequeue()
					cNode := cNode_.(*MBTNode)
					sSubNodes = append(sSubNodes, cNode)
					dDataHashes = append(dDataHashes, cNode.nodeHash)
				}
				queue.Enqueue(NewMBTNode([]byte("Branch"+strconv.Itoa(offset+i)), sSubNodes, dDataHashes, false, db, c))
			}
			offset += parSize
		}
	}
	rootHash := sha256.Sum256(root.nodeHash)
	mbtHash := rootHash[:]
	return &MBT{bucketNum, aggregation, gd, mbtHash, root.nodeHash, root, c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
}

func (mbt *MBT) GetRoot(db *leveldb.DB) *MBTNode {
	if mbt.Root == nil && len(mbt.rootHash) != 0 && mbt.updateLatch.TryLock() {
		if mbt.Root != nil {
			mbt.updateLatch.Unlock()
			return mbt.Root
		}
		if mbtString, err := db.Get(mbt.rootHash, nil); err == nil {
			m, _ := DeserializeMBTNode(mbtString)
			mbt.Root = m
		}
		mbt.updateLatch.Unlock()
	}
	for mbt.Root == nil && len(mbt.rootHash) != 0 {
	}
	return mbt.Root
}

func (mbt *MBT) GetRootHash() []byte {
	return mbt.rootHash
}

func (mbt *MBT) GetMBTHash() []byte {
	return mbt.mbtHash
}

func (mbt *MBT) GetBucketNum() int {
	return mbt.bucketNum
}

func (mbt *MBT) GetAggregation() int {
	return mbt.aggregation
}

func (mbt *MBT) GetGd() int {
	return mbt.gd
}

func (mbt *MBT) GetUpdateLatch() *sync.Mutex {
	return &mbt.updateLatch
}

func (mbt *MBT) Insert(kvPair util.KVPair, db *leveldb.DB) {
	mbt.GetRoot(db)            //保证根不是nil
	oldValueAddedFlag := false //防止读操作在已获得写锁的情况下获取锁
	mbt.latch.Lock()
	mbt.RecursivelyInsertMBTNode(ComputePath(mbt.bucketNum, mbt.aggregation, mbt.gd, kvPair.GetKey()), 0, kvPair, mbt.Root, db, &oldValueAddedFlag)
	mbt.UpdateMBTInDB(mbt.Root.nodeHash, db) //由于结构不变，因此根永远不会变动，变动的只会是根哈希，因此只需要向上更新mbt的树根哈希即可
	mbt.latch.Unlock()
}

func (mbt *MBT) RecursivelyInsertMBTNode(path []int, level int, kvPair util.KVPair, cNode *MBTNode, db *leveldb.DB, flag *bool) {
	cNode.latch.Lock()
	defer cNode.latch.Unlock()
	key := kvPair.GetKey()
	if flag != nil && !(*flag) { //只在最上层root阶段获取当前视图下mpt存有的value，保证获取的值最新
		oldVal, _ := mbt.QueryByKey(key, path, db, true)
		newVal := kvPair.GetValue()
		kvPair.SetValue(oldVal)
		isChange := kvPair.AddValue(newVal)
		if !isChange { //重复插入，直接返回
			return
		}
		*flag = true //其余层节点无需查询最新值
	}
	if level == len(path)-1 { //当前节点是叶节点
		isAdded := false
		for i, kv := range cNode.bucket { //有则追加
			if kv.GetKey() == key {
				cNode.bucket[i] = kvPair
				isAdded = true
				break
			}
		}
		if !isAdded { //无则新建
			cNode.bucket = append(cNode.bucket, kvPair)
			cNode.num++
			sort.Slice(cNode.bucket, func(i, j int) bool { //保证全局有序
				return strings.Compare(cNode.bucket[i].GetKey(), cNode.bucket[j].GetKey()) <= 0
			})
		}
		UpdateMBTNodeHash(cNode, -1, db, mbt.cache) //更新节点哈希并更新节点到db
	} else {
		mbt.RecursivelyInsertMBTNode(path, level+1, kvPair, cNode.subNodes[path[level+1]], db, flag)
		UpdateMBTNodeHash(cNode, path[level+1], db, mbt.cache) //更新己方保存的下层节点哈希并更新节点到db
	}
}

func (mbt *MBT) QueryByKey(key string, path []int, db *leveldb.DB, isLockFree bool) (string, *MBTProof) {
	if root := mbt.GetRoot(db); root == nil {
		return "", &MBTProof{false, nil}
	} else {
		//递归查询
		return mbt.RecursivelyQueryMBTNode(key, path, 0, root, db, isLockFree)
	}
}

func (mbt *MBT) RecursivelyQueryMBTNode(key string, path []int, level int, cNode *MBTNode, db *leveldb.DB, isLockFree bool) (string, *MBTProof) {
	if cNode == nil { //找不到
		return "", &MBTProof{false, nil}
	}
	if !isLockFree { //用于防止已获得写锁的读操作被自身写锁互斥
		cNode.latch.RLock()
		defer cNode.latch.RUnlock()
	}
	if level == len(path)-1 { //当前节点是叶节点
		proofElement := NewProofElement(level, 0, cNode.name, cNode.nodeHash, nil, nil)
		for _, kv := range cNode.bucket { //全桶扫描
			if kv.GetKey() == key {
				return kv.GetValue(), &MBTProof{true, []*ProofElement{proofElement}}
			}
		}
		return "", &MBTProof{false, []*ProofElement{proofElement}}
	} else {
		nextNode := cNode.GetSubNode(path[level+1], db, mbt.cache)
		var nextNodeHash []byte
		if nextNode != nil {
			nextNodeHash = nextNode.nodeHash
		}
		proofElement := NewProofElement(level, 1, cNode.name, cNode.nodeHash, nextNodeHash, cNode.dataHashes)
		valueStr, mbtProof := mbt.RecursivelyQueryMBTNode(key, path, level+1, nextNode, db, isLockFree)
		proofElements := append(mbtProof.GetProofs(), proofElement)
		return valueStr, &MBTProof{mbtProof.GetExist(), proofElements}
	}
}

// PrintQueryResult 打印查询结果
func (mbt *MBT) PrintQueryResult(key string, value string, mbtProof *MBTProof) {
	fmt.Printf("查询结果-------------------------------------------------------------------------------------------\n")
	fmt.Printf("key=%s\n", key)
	if value == "" {
		fmt.Printf("value不存在\n")
	} else {
		fmt.Printf("value=%s\n", value)
	}
	mbtProof.PrintMBTProof()
}

// VerifyQueryResult 验证查询结果
func (mbt *MBT) VerifyQueryResult(value string, mbtProof *MBTProof) bool {
	computedMBTRoot := ComputeMBTRoot(value, mbtProof)
	if !bytes.Equal(computedMBTRoot, mbt.Root.nodeHash) {
		fmt.Printf("根哈希值%x计算错误,验证不通过\n", computedMBTRoot)
		return false
	}
	fmt.Printf("根哈希值%x计算正确,验证通过\n", computedMBTRoot)
	return true
}

func ComputeMBTRoot(value string, mbtProof *MBTProof) []byte {
	proofs := mbtProof.GetProofs()
	nodeHash0 := []byte(value)
	nodeHash1 := make([]byte, 0)
	for _, proof := range proofs {
		switch proof.proofType {
		case 0:
			nodeHash1 = append(nodeHash1, proof.name...)
			if mbtProof.isExist {
				nodeHash1 = append(nodeHash1, []byte(value)...)
			} else { // 为减少字段，LeafNode的Value是childrenHashes的0号元素
				nodeHash1 = append(nodeHash1, proof.childrenHashes[0]...)
			}
			hash := sha256.Sum256(nodeHash1)
			nodeHash0 = hash[:]
			nodeHash1 = nil
		case 1:
			if !bytes.Equal(nodeHash0, proof.nextNodeHash) {
				fmt.Printf("level %d nextNodeHash=%x计算错误,验证不通过\n", proof.level, nodeHash0)
				return nil
			}
			nodeHash1 = append(nodeHash1, proof.name...)
			for _, childrenHash := range proof.childrenHashes {
				nodeHash1 = append(nodeHash1, childrenHash...)
			}
			hash := sha256.Sum256(nodeHash1)
			nodeHash0 = hash[:]
			nodeHash1 = nil
		default:
			log.Fatal("Unknown proofType " + strconv.Itoa(proof.proofType) + " in ComputeMBTRoot")
		}
	}
	return nodeHash0
}

func (mbt *MBT) UpdateMBTInDB(newRootHash []byte, db *leveldb.DB) {
	hash := sha256.Sum256(newRootHash)
	mbt.updateLatch.Lock()
	if err := db.Delete(mbt.mbtHash, nil); err != nil {
		panic(err)
	}
	mbt.mbtHash = hash[:]
	mbt.rootHash = newRootHash
	if err := db.Put(mbt.mbtHash, SerializeMBT(mbt), nil); err != nil {
		panic(err)
	}
	mbt.updateLatch.Unlock()
}

func (mbt *MBT) PurgeCache() {
	mbt.cache.Purge()
}

func callBackFoo[K comparable, V any](k K, v V, db *leveldb.DB) {
	k_, err := util.ToStringE(k)
	if err != nil {
		panic(err)
	}
	var v_ []byte
	switch any(v).(type) {
	case *MBTNode:
		v_ = SerializeMBTNode(any(v).(*MBTNode))
	default:
		panic("Unknown type " + reflect.TypeOf(v).String() + " in callBAckFoo in MBT.")
	}
	if err = db.Put([]byte(k_), v_, nil); err != nil {
		panic(err)
	}
}

func (mbt *MBT) PrintMBT(db *leveldb.DB) {
	fmt.Printf("打印MBT-------------------------------------------------------------------------------------------\n")
	if mbt == nil {
		return
	}
	//递归打印MGT
	mbt.latch.RLock() //mgt结构将不会更新，只会将未从磁盘中完全加载的结构从磁盘更新到内存结构中
	fmt.Printf("MBTRootHash: %x\n", mbt.rootHash)
	mbt.RecursivePrintMBTNode(mbt.GetRoot(db), 0, db)
	mbt.latch.RUnlock()
}

// RecursivePrintMBTNode 递归打印MGT
func (mbt *MBT) RecursivePrintMBTNode(node *MBTNode, level int, db *leveldb.DB) {
	if node == nil {
		return
	}
	fmt.Printf("Level: %d--------------------------------------------------------------------------\n", level)
	if node.isLeaf {
		fmt.Printf("Leaf Node: %s\n", hex.EncodeToString(node.nodeHash))
	} else {
		fmt.Printf("Internal Node: %s\n", hex.EncodeToString(node.nodeHash))
	}
	fmt.Printf("dataHashes:\n")
	for _, dataHash := range node.dataHashes {
		fmt.Printf("%s\n", hex.EncodeToString(dataHash))
	}
	for i := 0; i < len(node.dataHashes); i++ {
		if !node.isLeaf && node.dataHashes[i] != nil {
			mbt.RecursivePrintMBTNode(node.GetSubNode(i, db, mbt.cache), level+1, db)
		}
	}
}

type SeMBT struct {
	BucketNum   int
	Aggregation int
	Gd          int
	MBTRootHash []byte
}

func SerializeMBT(mbt *MBT) []byte {
	seMBT := &SeMBT{mbt.bucketNum, mbt.aggregation, mbt.gd, mbt.rootHash}
	if jsonMBT, err := json.Marshal(seMBT); err != nil {
		fmt.Printf("SerializeMGT error: %v\n", err)
		return nil
	} else {
		return jsonMBT
	}
}

func DeserializeMBT(data []byte, db *leveldb.DB, cacheEnable bool, mbtNodeCC int) (mbt *MBT, err error) {
	var seMBT SeMBT
	if err = json.Unmarshal(data, &seMBT); err != nil {
		fmt.Printf("DeserializeMBT error: %v\n", err)
		return nil, err
	}
	rootHash := sha256.Sum256(seMBT.MBTRootHash)
	mbtHash := rootHash[:]
	if cacheEnable {
		c, _ := lru.NewWithEvict[string, *MBTNode](mbtNodeCC, func(k string, v *MBTNode) {
			callBackFoo[string, *MBTNode](k, v, db)
		})
		mbt = &MBT{seMBT.BucketNum, seMBT.Aggregation, seMBT.Gd, mbtHash, seMBT.MBTRootHash, nil, c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	} else {
		mbt = &MBT{seMBT.BucketNum, seMBT.Aggregation, seMBT.Gd, mbtHash, seMBT.MBTRootHash, nil, nil, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	}
	return
}

func ComputePath(bucketNum int, aggregation int, gd int, key string) []int {
	key_ := key
	if len(key_) > 10 {
		key_ = key_[len(key_)-10:]
	}
	key__, _ := strconv.ParseInt(key_, 16, 64)
	cur := int(key__) % bucketNum
	return ComputePathFoo(aggregation, gd, cur, 0)
}

func ComputePathFoo(aggregation int, gd int, cur int, ld int) []int {
	if ld == gd-1 {
		return []int{-1}
	}
	return append(ComputePathFoo(aggregation, gd, cur/aggregation, ld+1), []int{cur % aggregation}...)
}