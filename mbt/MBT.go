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
	"sync"
)

type MBT struct {
	name        string
	bucketNum   int
	rdx         int
	rootHash    []byte
	Root        *MBTNode
	cache       *lru.Cache[string, *MBTNode]
	cacheEnable bool
	latch       sync.RWMutex
	updateLatch sync.Mutex
}

func NewMBT(name string, bucketNum int, rdx int, db *leveldb.DB, cacheEnable bool, mbtNodeCC int) *MBT {
	//此处就应该将全部的结构都初始化一遍
	var root *MBTNode
	if bucketNum <= 0 {
		panic("BucketNum of MBT must exceed 0.")
	} else if bucketNum == 1 {
		root = &MBTNode{nil, nil, make([][]byte, 0), true, make([]util.KVPair, 0), nil, sync.RWMutex{}, sync.Mutex{}}
	} else {
		root = &MBTNode{nil, make([]*MBTNode, rdx), make([][]byte, 0), false, nil, nil, sync.RWMutex{}, sync.Mutex{}}
		round := len(util.IntToHEXString(bucketNum - 1))
		queue := arrayqueue.New()
		queue.Enqueue(root)
		var curQueueSum int
		for i := 0; i < round; i++ {
			curQueueSum = queue.Size()
			isLeaf_ := i == round-1
			for j := 0; j < curQueueSum; j++ {
				cnode_, _ := queue.Dequeue()
				cnode, _ := cnode_.(*MBTNode)
				for k := 0; k < rdx; k++ {
					var ssubNode []*MBTNode
					var bbucket []util.KVPair
					if !isLeaf_ {
						ssubNode = make([]*MBTNode, rdx)
						bbucket = make([]util.KVPair, 0)
					}
					newNode := &MBTNode{nil, ssubNode, make([][]byte, 0), isLeaf_, bbucket, append([]int{k}, cnode.bucketKey...), sync.RWMutex{}, sync.Mutex{}}
					cnode.subNodes[j] = newNode
					if !isLeaf_ {
						queue.Enqueue(newNode)
					}
				}
			}
		}
	}
	if cacheEnable {
		c, _ := lru.NewWithEvict[string, *MBTNode](mbtNodeCC, func(k string, v *MBTNode) {
			callBackFoo[string, *MBTNode](k, v, db)
		})
		return &MBT{name, bucketNum, rdx, nil, root, c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	} else {
		return &MBT{name, bucketNum, rdx, nil, root, nil, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	}
}

func (mbt *MBT) GetRoot(db *leveldb.DB) *MBTNode {
	if mbt.Root == nil {
		mbt.updateLatch.Lock()
		defer mbt.updateLatch.Unlock()
		if mbt.Root != nil {
			return mbt.Root
		}
		if mbtString, err := db.Get(mbt.rootHash, nil); err == nil {
			m, _ := DeserializeMBTNode(mbtString, mbt.rdx)
			mbt.Root = m
		}
	}
	return mbt.Root
}

func (mbt *MBT) Insert(kvPair *util.KVPair, db *leveldb.DB) {
	// TODO

}

func (mbt *MBT) RecursivelyInsertMBTNode(level int, key string, value string, cnode *MBTNode, db *leveldb.DB, flag *bool) *MBTNode {
	// TODO
	cnode.latch.Lock()
	defer cnode.latch.Unlock()
	if flag != nil && !(*flag) {
		//val_, _ := mbt.QueryByKey()
	}
	return nil
}

func (mbt *MBT) RecursivelyQueryMBTNode(level int, bucketKey []int, cnode *MBTNode, db *leveldb.DB, isLockFree bool) (string, *MBTProof) {
	return "", nil
}

func (mbt *MBT) UpdateMBTInDB(newRootHash []byte, db *leveldb.DB) []byte {
	// TODO
	hash := sha256.Sum256(mbt.rootHash)
	if err := db.Delete(hash[:], nil); err != nil {
		panic(err)
	}
	mbt.rootHash = mbt.GetRoot(db).nodeHash
	hash = sha256.Sum256(mbt.rootHash)
	if err := db.Put(hash[:], SerializeMBT(mbt), nil); err != nil {
		panic(err)
	}
	return hash[:]
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
		fmt.Printf("bucketKey: %s\n", util.IntArrayToString(node.bucketKey, mbt.rdx))
	} else {
		fmt.Printf("Internal Node: %s\n", hex.EncodeToString(node.nodeHash))
	}
	fmt.Printf("dataHashes:\n")
	for _, dataHash := range node.dataHashes {
		fmt.Printf("%s\n", hex.EncodeToString(dataHash))
	}
	for i := 0; i < len(node.dataHashes); i++ {
		if !node.isLeaf && node.dataHashes[i] != nil {
			mbt.PrintMBTNode(mehtName, node.GetSubnode(i, db, mbt.rdx, cache), level+1, db, cache)
		}
	}
}

type SeMBT struct {
	Name string

	BucketNum   int
	Rdx         int
	MBTRootHash []byte
}

func SerializeMBT(mbt *MBT) []byte {
	seMBT := &SeMBT{mbt.name, mbt.bucketNum, mbt.rdx, mbt.rootHash}
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
		mbt = &MBT{seMBT.Name, seMBT.BucketNum, seMBT.Rdx, seMBT.MBTRootHash, nil, c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	} else {
		mbt = &MBT{seMBT.Name, seMBT.BucketNum, seMBT.Rdx, seMBT.MBTRootHash, nil, nil, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	}
	return
}
