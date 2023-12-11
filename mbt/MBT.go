package mbt

import (
	"MEHT/util"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/emirpasic/gods/queues/arrayqueue"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type MBT struct {
	name        string
	bucketNum   int
	aggr        int
	offset      int
	rootHash    []byte
	Root        *MBTNode
	cache       *lru.Cache[string, *MBTNode]
	cacheEnable bool
	latch       sync.RWMutex
	updateLatch sync.Mutex
}

func NewMBT(name string, bucketNum int, aggr int, db *leveldb.DB, cacheEnable bool, mbtNodeCC int) *MBT {
	//此处就应该将全部的结构都初始化一遍
	var root *MBTNode
	var offset = 0
	var c *lru.Cache[string, *MBTNode]
	if cacheEnable {
		c, _ = lru.NewWithEvict[string, *MBTNode](mbtNodeCC, func(k string, v *MBTNode) {
			callBackFoo[string, *MBTNode](k, v, db)
		})
	}
	if bucketNum <= 0 {
		panic("BucketNum of MBT must exceed 0.")
	} else if bucketNum == 1 {
		offset++
		root = NewMBTNode([]byte("Root"), nil, make([][]byte, 1), true, db, c)
	} else {
		queue := arrayqueue.New()
		for i := 0; i < bucketNum; i++ {
			queue.Enqueue(NewMBTNode([]byte("LeafNode"+strconv.Itoa(i)), nil, make([][]byte, 1), true, db, c))
		}
		for !queue.Empty() {
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
				db.Delete(oldHash, nil)
				break
			}
			parSize := s / aggr
			if s%aggr != 0 {
				parSize++
			}
			for i := 0; i < parSize; i++ {
				ssubNodes := make([]*MBTNode, 0)
				ddataHashes := make([][]byte, 0)
				for j := 0; j < aggr && !queue.Empty(); j++ {
					cnode_, _ := queue.Dequeue()
					cnode := cnode_.(*MBTNode)
					ssubNodes = append(ssubNodes, cnode)
					ddataHashes = append(ddataHashes, cnode.nodeHash)
				}
				queue.Enqueue(NewMBTNode([]byte("Branch"+strconv.Itoa(offset+i)), ssubNodes, ddataHashes, false, db, c))
			}
			offset += parSize
		}
	}
	return &MBT{name, bucketNum, aggr, offset, root.nodeHash, root, c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
}

func (mbt *MBT) GetRoot(db *leveldb.DB) *MBTNode {
	if mbt.Root == nil {
		mbt.updateLatch.Lock()
		defer mbt.updateLatch.Unlock()
		if mbt.Root != nil {
			return mbt.Root
		}
		if mbtString, err := db.Get(mbt.rootHash, nil); err == nil {
			m, _ := DeserializeMBTNode(mbtString)
			mbt.Root = m
		}
	}
	return mbt.Root
}

func (mbt *MBT) GetRootHash() []byte {
	return mbt.rootHash
}

func (mbt *MBT) Insert(kvPair util.KVPair, db *leveldb.DB) {
	mbt.GetRoot(db)
	oldValueAddedFlag := false
	mbt.RecursivelyInsertMBTNode(ComputePath(mbt.bucketNum, mbt.offset, mbt.aggr, kvPair.GetKey()), 0, &kvPair, mbt.Root, db, &oldValueAddedFlag)
	mbt.UpdateMBTInDB(mbt.Root.nodeHash, db)
}

func (mbt *MBT) RecursivelyInsertMBTNode(path []int, level int, kvPair *util.KVPair, cnode *MBTNode, db *leveldb.DB, flag *bool) {
	cnode.latch.Lock()
	defer cnode.latch.Unlock()
	key := kvPair.GetKey()
	if flag != nil && !(*flag) {
		val, _ := mbt.QueryByKey(key, path, db, true)
		isChange := kvPair.AddValue(val)
		if !isChange {
			return
		}
		*flag = true
	}
	if level == len(path)-1 { //当前节点是叶节点
		isAdded := false
		for i, kv := range cnode.bucket {
			if kv.GetKey() == key {
				cnode.bucket[i] = *kvPair
				isAdded = true
			}
			if !isAdded {
				cnode.bucket = append(cnode.bucket, *kvPair)
				sort.Slice(cnode.bucket, func(i, j int) bool {
					return strings.Compare(cnode.bucket[i].GetKey(), cnode.bucket[j].GetKey()) <= 0
				})
			}
			UpdateMBTNodeHash(cnode, -1, db, mbt.cache)
		}
	} else {
		mbt.RecursivelyInsertMBTNode(path, level+1, kvPair, cnode.subNodes[path[level+1]], db, flag)
		UpdateMBTNodeHash(cnode, path[level+1], db, mbt.cache)
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

func (mbt *MBT) RecursivelyQueryMBTNode(key string, path []int, level int, cnode *MBTNode, db *leveldb.DB, isLockFree bool) (string, *MBTProof) {
	if cnode == nil {
		return "", &MBTProof{false, nil}
	}
	if !isLockFree {
		cnode.latch.RLock()
		defer cnode.latch.RUnlock()
	}
	if level == len(path)-1 { //当前节点是叶节点
		proofElement := NewProofElement(level, 0, cnode.nodeHash, nil, nil)
		for _, kv := range cnode.bucket {
			if kv.GetKey() == key {
				return kv.GetValue(), &MBTProof{true, []*ProofElement{proofElement}}
			}
		}
		return "", &MBTProof{false, []*ProofElement{proofElement}}
	} else {
		proofElement := NewProofElement(level, 1, cnode.nodeHash, cnode.subNodes[path[level+1]].nodeHash, cnode.dataHashes)
		valueStr, mbtProof := mbt.RecursivelyQueryMBTNode(key, path, level+1, cnode.subNodes[path[level+1]], db, isLockFree)
		proofElements := append(mbtProof.GetProofs(), proofElement)
		return valueStr, &MBTProof{mbtProof.GetExist(), proofElements}
	}
}

func (mbt *MBT) UpdateMBTInDB(newRootHash []byte, db *leveldb.DB) {
	hash := sha256.Sum256(mbt.rootHash)
	if err := db.Delete(hash[:], nil); err != nil {
		panic(err)
	}
	mbt.rootHash = newRootHash
	hash = sha256.Sum256(mbt.rootHash)
	if err := db.Put(hash[:], SerializeMBT(mbt), nil); err != nil {
		panic(err)
	}
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

func (mbt *MBT) PrintMGT(mehtName string, db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) {
	fmt.Printf("打印MBT-------------------------------------------------------------------------------------------\n")
	if mbt == nil {
		return
	}
	//递归打印MGT
	mbt.latch.RLock() //mgt结构将不会更新，只会将未从磁盘中完全加载的结构从磁盘更新到内存结构中
	fmt.Printf("MBTRootHash: %x\n", mbt.rootHash)
	mbt.PrintMBTNode(mehtName, mbt.GetRoot(db), 0, db, cache)
	mbt.latch.RUnlock()
}

// 递归打印MGT
func (mbt *MBT) PrintMBTNode(mehtName string, node *MBTNode, level int, db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) {
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
			mbt.PrintMBTNode(mehtName, node.GetSubnode(i, db, cache), level+1, db, cache)
		}
	}
}

type SeMBT struct {
	Name string

	BucketNum int
	aggr      int
	offset    int

	MBTRootHash []byte
}

func SerializeMBT(mbt *MBT) []byte {
	seMBT := &SeMBT{mbt.name, mbt.bucketNum, mbt.aggr, mbt.offset, mbt.rootHash}
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
	if cacheEnable {
		c, _ := lru.NewWithEvict[string, *MBTNode](mbtNodeCC, func(k string, v *MBTNode) {
			callBackFoo[string, *MBTNode](k, v, db)
		})
		mbt = &MBT{seMBT.Name, seMBT.BucketNum, seMBT.aggr, seMBT.offset, seMBT.MBTRootHash, nil, c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	} else {
		mbt = &MBT{seMBT.Name, seMBT.BucketNum, seMBT.aggr, seMBT.offset, seMBT.MBTRootHash, nil, nil, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	}
	return
}

func ComputePath(bucketNum int, offset int, aggr int, key string) []int {
	var key_ = key
	if len(key_) > 5 {
		key_ = key_[:5]
	}
	key__, _ := strconv.ParseInt(key_, 16, 64)
	cur := int(key__) % bucketNum
	return ComputePathFoo(aggr, cur+offset)
}

func ComputePathFoo(aggr int, cur int) []int {
	if cur == 0 {
		return []int{-1}
	}
	return append(ComputePathFoo(aggr, (cur-1)/aggr), []int{(cur - 1) % aggr}...)
}
