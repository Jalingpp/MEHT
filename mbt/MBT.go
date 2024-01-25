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
			for i := 0; i < parSize && s > 0; i++ {
				sSubNodes := make([]*MBTNode, 0)
				dDataHashes := make([][]byte, 0)
				for j := 0; j < aggregation && !queue.Empty() && s > 0; j++ {
					cNode_, _ := queue.Dequeue()
					cNode := cNode_.(*MBTNode)
					sSubNodes = append(sSubNodes, cNode)
					dDataHashes = append(dDataHashes, cNode.nodeHash)
					s--
				}
				queue.Enqueue(NewMBTNode([]byte("Branch"+strconv.Itoa(offset+i)), sSubNodes, dDataHashes, false, db, c))
			}
			offset += parSize
		}
	}
	return &MBT{bucketNum, aggregation, gd, root.nodeHash, root, c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
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

func (mbt *MBT) Insert(kvPair util.KVPair, db *leveldb.DB, isDelete bool) {
	mbt.GetRoot(db) //保证根不是nil
	path := ComputePath(mbt.bucketNum, mbt.aggregation, mbt.gd, kvPair.GetKey())
	mbt.RecursivelyInsertMBTNode(path, 0, kvPair, mbt.Root, db, isDelete)
	//mbt.UpdateMBTInDB(mbt.Root.nodeHash, db) //由于结构不变，因此根永远不会变动，变动的只会是根哈希，因此只需要向上更新mbt的树根哈希即可
}

func (mbt *MBT) RecursivelyInsertMBTNode(path []int, level int, kvPair util.KVPair, cNode *MBTNode, db *leveldb.DB, isDelete bool) {
	key := kvPair.GetKey()
	if level == len(path)-1 { //当前节点是叶节点
		//只锁叶子节点，其余路径不锁
		cNode.latch.Lock()
		defer cNode.latch.Unlock()
		var oldVal string
		var pos = -1
		for idx, kv := range cNode.bucket { //全桶扫描
			if kv.GetKey() == key {
				oldVal = kv.GetValue()
				pos = idx
				break
			}
		}
		newVal := kvPair.GetValue()
		kvPair.SetValue(oldVal)
		if isDelete {
			if pos == -1 { //删除失败，将要删除的kv队加入延迟删除记录表
				cNode.toDelMap[kvPair.GetKey()][newVal]++
				return
			} else {
				if isChange := kvPair.DelValue(newVal); !isChange { //删除失败，将要删除的kv队加入延迟删除记录表
					cNode.toDelMap[kvPair.GetKey()][newVal]++
					return
				} else {
					cNode.bucket[pos] = kvPair
				}
			}
		} else {
			if cNode.toDelMap[kvPair.GetKey()][newVal] > 0 { //删除记录表中有则删除，即跳过本次插入操作
				cNode.toDelMap[kvPair.GetKey()][newVal]--
				return
			}
			isChange := kvPair.AddValue(newVal)
			if !isChange { //重复插入，直接返回
				return
			}
			if pos != -1 { //key有则用新值覆盖
				cNode.bucket[pos] = kvPair
			} else { //无则新建
				cNode.bucket = append(cNode.bucket, kvPair)
				cNode.num++
				sort.Slice(cNode.bucket, func(i, j int) bool { //保证全局有序
					return strings.Compare(cNode.bucket[i].GetKey(), cNode.bucket[j].GetKey()) <= 0
				})
			}
		}
		cNode.UpdateMBTNodeHash(db, mbt.cache) //更新节点哈希并更新节点到db
		tmpNode := cNode.parent
		if tmpNode != nil {
			tmpNode.dataHashes[path[level]] = cNode.nodeHash
		}
		for tmpNode != nil {
			if tmpNode.isDirty {
				break
			}
			tmpNode.isDirty = true
			tmpNode = tmpNode.parent
		}
	} else {
		nextNode := cNode.GetSubNode(path[level+1], db, mbt.cache)
		mbt.RecursivelyInsertMBTNode(path, level+1, kvPair, nextNode, db, isDelete)
		//UpdateMBTNodeHash(cNode, path[level+1], db, mbt.cache) //更新己方保存的下层节点哈希并更新节点到db
	}
}

func (mbt *MBT) QueryByKey(key string, path []int, db *leveldb.DB) (string, *MBTProof) {
	if root := mbt.GetRoot(db); root == nil {
		return "", &MBTProof{false, nil}
	} else {
		//递归查询
		return mbt.RecursivelyQueryMBTNode(key, path, 0, root, db)
	}
}

func (mbt *MBT) RecursivelyQueryMBTNode(key string, path []int, level int, cNode *MBTNode, db *leveldb.DB) (string, *MBTProof) {
	if cNode == nil { //找不到
		return "", &MBTProof{false, nil}
	}
	if level == len(path)-1 { //当前节点是叶节点
		cNode.latch.RLock()
		defer cNode.latch.RUnlock()
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
		valueStr, mbtProof := mbt.RecursivelyQueryMBTNode(key, path, level+1, nextNode, db)
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
	oldHash := sha256.Sum256(mbt.rootHash)
	if err := db.Delete(oldHash[:], nil); err != nil {
		panic(err)
	}
	mbt.rootHash = newRootHash
	if err := db.Put(hash[:], SerializeMBT(mbt), nil); err != nil {
		panic(err)
	}
	mbt.updateLatch.Unlock()
}

// MBTBatchFix 对根节点的所有孩子节点做并发，调用递归函数更新脏节点
func (mbt *MBT) MBTBatchFix(db *leveldb.DB) {
	if mbt.Root == nil || !mbt.Root.isDirty {
		return
	}
	wG := sync.WaitGroup{}
	for i, child := range mbt.Root.subNodes {
		if child == nil || !child.isDirty { //如果孩子节点为空或者孩子节点不是脏节点，则跳过
			continue
		}
		wG.Add(1)
		child_ := child
		idx := i
		go func() {
			mbtBatchFixFoo(child_, db, mbt.cache)
			mbt.Root.dataHashes[idx] = child_.nodeHash
			wG.Done()
		}()
	}
	wG.Wait()
	mbt.Root.UpdateMBTNodeHash(db, mbt.cache)
	//fmt.Println("--------------------------------------")
	//for i, hash := range mbt.Root.dataHashes {
	//	fmt.Println("rootChild", i, ": ", hash)
	//}
	//fmt.Println("--------------------------------------")
	mbt.Root.isDirty = false
	mbt.UpdateMBTInDB(mbt.Root.nodeHash, db)
}

// mbtBatchFixFoo 递归更新脏节点
func mbtBatchFixFoo(mbtNode *MBTNode, db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) {
	if mbtNode == nil || !mbtNode.isDirty {
		return
	}
	for idx, child := range mbtNode.subNodes {
		if child == nil || !child.isDirty {
			continue
		}
		mbtBatchFixFoo(child, db, cache)
		mbtNode.dataHashes[idx] = child.nodeHash
	}
	mbtNode.UpdateMBTNodeHash(db, cache)
	mbtNode.isDirty = false
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
	if cacheEnable {
		c, _ := lru.NewWithEvict[string, *MBTNode](mbtNodeCC, func(k string, v *MBTNode) {
			callBackFoo[string, *MBTNode](k, v, db)
		})
		mbt = &MBT{seMBT.BucketNum, seMBT.Aggregation, seMBT.Gd, seMBT.MBTRootHash, nil, c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	} else {
		mbt = &MBT{seMBT.BucketNum, seMBT.Aggregation, seMBT.Gd, seMBT.MBTRootHash, nil, nil, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
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
