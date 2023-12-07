package mpt

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
)

//MPT节点相关的结构体和方法
//func (fn *FullNode) GetChildInFullNode(index int, db *leveldb.DB) *ShortNode {}:获取FullNode的第index个child，如果为nil，则从数据库中查询
//func (sn *ShortNode) GetNextNode(db *leveldb.DB) *FullNode {}： 获取ShortNode的nextNode，如果为nil，则从数据库中查询
//func NewShortNode(prefix []byte, isLeaf bool, suffix []byte, nextNode *FullNode, value []byte, db *leveldb.DB) *ShortNode {}: creates a ShortNode and computes its nodeHash
//func UpdateShortNodeHash(sn *ShortNode, db *leveldb.DB) {}：更新ShortNode的nodeHash
//func NewFullNode(children [16]*ShortNode, db *leveldb.DB) *FullNode {}: creates a new FullNode and computes its nodeHash
//func UpdateFullNodeHash(fn *FullNode, db *leveldb.DB) {}: updates the nodeHash of a FullNode
//func SerializeShortNode(sn *ShortNode) []byte {}： 序列化ShortNode
//func DeserializeShortNode(ssnstring []byte) (*ShortNode, error) {}：反序列化ShortNode
//func SerializeFullNode(fn *FullNode) []byte {}：序列化FullNode
// func DeserializeFullNode(sfnstring []byte) (*FullNode, error) {}：反序列化FullNode

type FullNode struct {
	nodeHash     []byte         //当前节点的哈希值,由childrenHash计算得到
	children     [16]*ShortNode // children的指针，0-15表示0-9,a-f
	childrenHash [16][]byte     // children的哈希值
	value        []byte         // 以前面ExtensionNode的prefix+suffix为key的value
	latch        sync.RWMutex
	updateLatch  sync.Mutex
}

// 获取FullNode的第index个child，如果为nil，则从数据库中查询
func (fn *FullNode) GetChildInFullNode(index int, db *leveldb.DB, cache *[]interface{}) *ShortNode {
	//如果当前节点的children[index]为nil，则从数据库中查询
	if fn.GetChildren()[index] == nil {
		var ok bool
		var child *ShortNode
		fn.updateLatch.Lock() // 由于不一定一定有child，因此每一次获取都是串行的
		defer fn.updateLatch.Unlock()
		if fn.GetChildren()[index] != nil { // 可能刚好在进到if但是lock之前别的线程更新了孩子
			return fn.GetChildren()[index]
		}
		if cache != nil {
			targetCache, _ := (*cache)[0].(*lru.Cache[string, *ShortNode])
			if child, ok = targetCache.Get(string(fn.GetChildrenHash()[index])); ok {
				fn.SetChild(index, child)
			}
		}
		if !ok {
			if childString, _ := db.Get(fn.GetChildrenHash()[index], nil); len(childString) != 0 {
				child, _ = DeserializeShortNode(childString)
				fn.SetChild(index, child)
			}
		}

	}
	return fn.GetChildren()[index]
}

type ShortNode struct {
	nodeHash []byte //当前节点的哈希值，由prefix+suffix+value/nextNodeHash计算得到

	prefix string //前缀

	isLeaf       bool      //是否是叶子节点
	suffix       string    //后缀，shared nibble（extension node）或key-end（leaf node）
	nextNode     *FullNode //下一个FullNode节点(当前节点是extension node时)
	nextNodeHash []byte    //下一个FullNode节点的哈希值
	value        []byte    //value（当前节点是leaf node时）
	latch        sync.RWMutex
	updateLatch  sync.Mutex
}

// 获取ShortNode的nextNode，如果为nil，则从数据库中查询
func (sn *ShortNode) GetNextNode(db *leveldb.DB, cache *[]interface{}) *FullNode {
	//如果当前节点的nextNode为nil，则从数据库中查询
	if sn.nextNode == nil && len(sn.nextNodeHash) != 0 && sn.updateLatch.TryLock() { // 只允许一个线程重构nextNode
		if sn.nextNode != nil {
			sn.updateLatch.Unlock()
			return sn.nextNode
		}
		var ok bool
		var nextNode *FullNode
		if cache != nil {
			targetCache, _ := (*cache)[1].(*lru.Cache[string, *FullNode])
			if nextNode, ok = targetCache.Get(string(sn.nextNodeHash)); ok {
				sn.nextNode = nextNode
			}
		}
		if !ok {
			if nextNodeString, error_ := db.Get(sn.nextNodeHash, nil); error_ == nil {
				nextNode, _ = DeserializeFullNode(nextNodeString)
				sn.nextNode = nextNode
			}
		}
		sn.updateLatch.Unlock()
	}
	for sn.nextNode == nil && len(sn.nextNodeHash) != 0 {
	} // 其余线程等待nextNode重构
	return sn.nextNode
}

// NewShortNode creates a ShortNode and computes its nodeHash
func NewShortNode(prefix string, isLeaf bool, suffix string, nextNode *FullNode, value []byte, db *leveldb.DB, cache *[]interface{}) *ShortNode {
	nodeHash := append([]byte(prefix), suffix...)
	var nextNodeHash []byte
	if isLeaf {
		nodeHash = append(nodeHash, value...)
		nextNodeHash = nil
	} else {
		nextNodeHash = nextNode.nodeHash
		nodeHash = append(nodeHash, nextNodeHash...)
	}
	hash := sha256.Sum256(nodeHash)
	nodeHash = hash[:]
	sn := &ShortNode{nodeHash, prefix, isLeaf, suffix, nextNode, nextNodeHash, value, sync.RWMutex{}, sync.Mutex{}}
	//将sn写入db中
	if cache != nil {
		targetCache, _ := (*cache)[0].(*lru.Cache[string, *ShortNode])
		targetCache.Add(string(sn.nodeHash), sn)
	} else {
		ssn := SerializeShortNode(sn)
		if err := db.Put(sn.nodeHash, ssn, nil); err != nil {
			fmt.Println("Insert ShortNode to DB error:", err)
		}
	}
	return sn
}

// UpdateShortNodeHash 更新ShortNode的nodeHash
func UpdateShortNodeHash(sn *ShortNode, db *leveldb.DB, cache *[]interface{}) {
	//先删除db中原有节点(考虑到新增的其他ShortNode可能与旧ShortNode的nodeHash相同，删除可能会丢失数据，所以注释掉)
	// err := db.Delete(sn.nodeHash, nil)
	// if err != nil {
	// 	fmt.Println("Delete ShortNode from DB error:", err)
	// }
	nodeHash := append([]byte(sn.prefix), sn.suffix...)
	if sn.isLeaf {
		nodeHash = append(nodeHash, sn.value...)
	} else {
		nodeHash = append(nodeHash, sn.nextNode.nodeHash...)
	}
	hash := sha256.Sum256(nodeHash)
	sn.nodeHash = hash[:]
	if cache != nil {
		targetCache, _ := (*cache)[0].(*lru.Cache[string, *ShortNode])
		targetCache.Add(string(sn.nodeHash), sn)
	} else {
		ssn := SerializeShortNode(sn)
		//将更新后的sn写入db中
		if err := db.Put(sn.nodeHash, ssn, nil); err != nil {
			fmt.Println("Insert ShortNode to DB error:", err)
		}
	}
}

// NewFullNode creates a new FullNode and computes its nodeHash
func NewFullNode(children [16]*ShortNode, value []byte, db *leveldb.DB, cache *[]interface{}) *FullNode {
	var childrenHash [16][]byte
	var nodeHash []byte
	for i := 0; i < 16; i++ {
		if children[i] == nil {
			childrenHash[i] = nil
		} else {
			childrenHash[i] = children[i].nodeHash
		}
		nodeHash = append(nodeHash, childrenHash[i]...)
	}
	nodeHash = append(nodeHash, value...)
	hash := sha256.Sum256(nodeHash)
	nodeHash = hash[:]
	fn := &FullNode{nodeHash, children, childrenHash, value, sync.RWMutex{}, sync.Mutex{}}
	//将fn写入db中
	if cache != nil {
		targetCache, _ := ((*cache)[1]).(*lru.Cache[string, *FullNode])
		targetCache.Add(string(fn.nodeHash), fn)
	} else {
		sfn := SerializeFullNode(fn)
		if err := db.Put(fn.nodeHash, sfn, nil); err != nil {
			fmt.Println("Insert FullNode to DB error:", err)
		}
	}
	return fn
}

// UpdateFullNodeHash updates the nodeHash of a FullNode
func UpdateFullNodeHash(fn *FullNode, db *leveldb.DB, cache *[]interface{}) {
	////先删除db中原有节点
	//if err := db.Delete(fn.nodeHash, nil); err != nil {
	//	fmt.Println("Delete FullNode from DB error:", err)
	//}
	nodeHash := make([]byte, 0)
	for i := 0; i < 16; i++ {
		nodeHash = append(nodeHash, fn.childrenHash[i]...)
	}
	nodeHash = append(nodeHash, fn.value...)
	hash := sha256.Sum256(nodeHash)
	fn.nodeHash = hash[:]
	//将更新后的fn写入db中
	if cache != nil {
		targetCache, _ := (*cache)[1].(*lru.Cache[string, *FullNode])
		targetCache.Add(string(fn.nodeHash), fn)
	} else {
		sfn := SerializeFullNode(fn)
		if err := db.Put(fn.nodeHash, sfn, nil); err != nil {
			fmt.Println("Insert FullNode to DB error:", err)
		}
	}
}

type SeShortNode struct {
	NodeHash     []byte //当前节点的哈希值，由prefix+suffix+value/nextNodeHash计算得到
	Prefix       string //前缀
	IsLeaf       bool   //是否是叶子节点
	Suffix       string //后缀，shared nibble（extension node）或key-end（leaf node）
	NextNodeHash []byte //下一个FullNode节点的哈希值
	Value        []byte //value（当前节点是leaf node时）
}

// 序列化ShortNode
func SerializeShortNode(sn *ShortNode) []byte {
	ssn := &SeShortNode{sn.GetNodeHash(), sn.GetPrefix(), sn.GetIsLeaf(), sn.GetSuffix(), sn.GetNextNodeHash(), sn.GetValue()}
	if jsonSSN, err := json.Marshal(ssn); err != nil {
		fmt.Printf("SerializeShortNode error: %v\n", err)
		return nil
	} else {
		return jsonSSN
	}
}

// 反序列化ShortNode
func DeserializeShortNode(ssnstring []byte) (*ShortNode, error) {
	var ssn SeShortNode
	if err := json.Unmarshal(ssnstring, &ssn); err != nil {
		fmt.Printf("DeserializeShortNode error: %v\n", err)
		return nil, err
	}
	sn := &ShortNode{nil, ssn.Prefix, ssn.IsLeaf, ssn.Suffix, nil, nil, ssn.Value, sync.RWMutex{}, sync.Mutex{}}
	sn.nodeHash = ssn.NodeHash
	sn.SetNextNodeHash(ssn.NextNodeHash)
	return sn, nil
}

type SeFullNode struct {
	NodeHash     []byte     //当前节点的哈希值,由childrenHash计算得到
	ChildrenHash [16][]byte // children的哈希值
	Value        []byte     // 以前面ExtensionNode的prefix+suffix为key的value
}

// 序列化FullNode
func SerializeFullNode(fn *FullNode) []byte {
	sfn := &SeFullNode{fn.GetNodeHash(), fn.GetChildrenHash(), fn.GetValue()}
	if jsonSFN, err := json.Marshal(sfn); err != nil {
		fmt.Printf("SerializeFullNode error: %v\n", err)
		return nil
	} else {
		return jsonSFN
	}
}

// 反序列化FullNode
func DeserializeFullNode(sfnstring []byte) (*FullNode, error) {
	var sfn SeFullNode
	if err := json.Unmarshal(sfnstring, &sfn); err != nil {
		fmt.Printf("DeserializeFullNode error: %v\n", err)
		return nil, err
	}
	var children [16]*ShortNode
	for i := 0; i < 16; i++ {
		children[i] = nil
	}
	fn := &FullNode{sfn.NodeHash, children, sfn.ChildrenHash, sfn.Value, sync.RWMutex{}, sync.Mutex{}}
	return fn, nil
}

func (sn *ShortNode) GetNodeHash() []byte {
	return sn.nodeHash
}

func (sn *ShortNode) GetPrefix() string {
	return sn.prefix
}

func (sn *ShortNode) GetSuffix() string {
	return sn.suffix
}

func (sn *ShortNode) GetValue() []byte {
	return sn.value
}

func (sn *ShortNode) GetNextNodeHash() []byte {
	return sn.nextNodeHash
}

func (sn *ShortNode) GetIsLeaf() bool {
	return sn.isLeaf
}

func (sn *ShortNode) SetNextNodeHash(nnh []byte) {
	sn.nextNodeHash = nnh
}

func (fn *FullNode) GetNodeHash() []byte {
	return fn.nodeHash
}

func (fn *FullNode) GetChildren() [16]*ShortNode {
	return fn.children
}

func (fn *FullNode) GetChildrenHash() [16][]byte {
	return fn.childrenHash
}

func (fn *FullNode) SetChild(index int, sn *ShortNode) {
	fn.children[index] = sn
}

func (fn *FullNode) GetValue() []byte {
	return fn.value
}
