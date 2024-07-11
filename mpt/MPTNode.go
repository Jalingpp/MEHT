package mpt

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
)

type FullNode struct {
	nodeHash     [32]byte                  //当前节点的哈希值,由childrenHash计算得到
	parent       *ShortNode                //父节点
	children     [16]*ShortNode            // children的指针，0-15表示0-9,a-f
	childrenHash [16][]byte                // children的哈希值
	value        []byte                    // 以前面ExtensionNode的prefix+suffix为key的value
	isDirty      bool                      //是否修改但未提交
	toDelMap     map[string]map[string]int //因为更新而待删除的辅助索引值
	latch        sync.RWMutex
	childLatch   [16]sync.RWMutex
	updateLatch  sync.Mutex
}

// GetChildInFullNode 获取FullNode的第index个child，如果为nil，则从数据库中查询
func (fn *FullNode) GetChildInFullNode(index int, db *leveldb.DB, cache *[]interface{}) *ShortNode {
	//如果当前节点的children[index]为nil，则从数据库中查询
	if fn.GetChildren()[index] == nil && fn.GetChildrenHash()[index] != nil {
		if fn.GetChildren()[index] != nil { // 可能刚好在进到if但是lock之前别的线程更新了孩子
			return fn.GetChildren()[index]
		}
		var ok bool
		var child *ShortNode
		if cache != nil {
			targetCache, _ := (*cache)[0].(*lru.Cache[string, *ShortNode])
			hash_ := fn.GetChildrenHash()[index]
			if child, ok = targetCache.Get(string(hash_[:])); ok {
				fn.SetChild(index, child)
			}
		}
		if !ok {
			hash_ := fn.GetChildrenHash()[index]
			if childString, _ := db.Get(hash_[:], nil); len(childString) != 0 {
				child, _ = DeserializeShortNode(childString)
				fn.SetChild(index, child)
			}
		}
	}
	return fn.GetChildren()[index]
}

type ShortNode struct {
	nodeHash [32]byte //当前节点的哈希值，由prefix+suffix+value/nextNodeHash计算得到

	prefix string //前缀
	parent *FullNode

	isLeaf       bool                      //是否是叶子节点
	isDirty      bool                      //是否修改但未提交
	suffix       string                    //后缀，shared nibble（extension node）或key-end（leaf node）
	nextNode     *FullNode                 //下一个FullNode节点(当前节点是extension node时)
	nextNodeHash [32]byte                  //下一个FullNode节点的哈希值
	value        []byte                    //value（当前节点是leaf node时）
	toDelMap     map[string]map[string]int //因为更新而待删除的辅助索引值
	latch        sync.RWMutex
	updateLatch  sync.Mutex
}

// GetNextNode 获取ShortNode的nextNode，如果为nil，则从数据库中查询
func (sn *ShortNode) GetNextNode(db *leveldb.DB, cache *[]interface{}) *FullNode {
	//如果当前节点的nextNode为nil，则从数据库中查询
	if sn.nextNode == nil && sn.nextNodeHash != [32]byte{} && sn.updateLatch.TryLock() { // 只允许一个线程重构nextNode
		if sn.nextNode != nil {
			sn.updateLatch.Unlock()
			return sn.nextNode
		}
		var ok bool
		var nextNode *FullNode
		if cache != nil {
			targetCache, _ := (*cache)[1].(*lru.Cache[string, *FullNode])
			if nextNode, ok = targetCache.Get(string(sn.nextNodeHash[:])); ok {
				sn.nextNode = nextNode
				nextNode.parent = sn
			}
		}
		if !ok {
			if nextNodeString, _ := db.Get(sn.nextNodeHash[:], nil); len(nextNodeString) != 0 {
				nextNode, _ = DeserializeFullNode(nextNodeString)
				sn.nextNode = nextNode
				nextNode.parent = sn
			}
		}
		sn.updateLatch.Unlock()
	}
	for sn.nextNode == nil && sn.nextNodeHash != [32]byte{} {
	} // 其余线程等待nextNode重构
	return sn.nextNode
}

// NewShortNode creates a ShortNode and computes its nodeHash
func NewShortNode(prefix string, isLeaf bool, suffix string, nextNode *FullNode, value []byte, db *leveldb.DB, cache *[]interface{}) *ShortNode {
	sn := &ShortNode{[32]byte{}, prefix, nil, isLeaf, false, suffix, nextNode, [32]byte{},
		value, make(map[string]map[string]int), sync.RWMutex{}, sync.Mutex{}}
	nodeHash := append([]byte(prefix), suffix...)
	var nextNodeHash [32]byte
	if isLeaf {
		nodeHash = append(nodeHash, value...)
		nextNodeHash = [32]byte{}
	} else {
		nextNode.parent = sn
		nextNodeHash = nextNode.nodeHash
		nodeHash = append(nodeHash, nextNodeHash[:]...)
	}
	sn.nodeHash = sha256.Sum256(nodeHash)
	sn.nextNodeHash = nextNodeHash
	//将sn写入db中
	if cache != nil {
		targetCache, _ := (*cache)[0].(*lru.Cache[string, *ShortNode])
		targetCache.Add(string(sn.nodeHash[:]), sn)
	} else {
		ssn := SerializeShortNode(sn)
		if err := db.Put(sn.nodeHash[:], ssn, nil); err != nil {
			fmt.Println("Insert ShortNode to DB error:", err)
		}
	}
	return sn
}

// UpdateShortNodeHash 更新ShortNode的nodeHash
func (sn *ShortNode) UpdateShortNodeHash(db *leveldb.DB, cache *[]interface{}) {
	//先删除db中原有节点(考虑到新增的其他ShortNode可能与旧ShortNode的nodeHash相同，删除可能会丢失数据，所以注释掉)
	if cache != nil {
		targetCache, _ := (*cache)[0].(*lru.Cache[string, *ShortNode])
		targetCache.Remove(string(sn.nodeHash[:]))
	}
	err := db.Delete(sn.nodeHash[:], nil)
	if err != nil {
		fmt.Println("Delete ShortNode from DB error:", err)
	}
	nodeHash := append([]byte(sn.prefix), sn.suffix...)
	if sn.isLeaf {
		nodeHash = append(nodeHash, sn.value...)
	} else {
		nodeHash = append(nodeHash, sn.nextNodeHash[:]...)
	}
	sn.nodeHash = sha256.Sum256(nodeHash)
	if cache != nil {
		targetCache, _ := (*cache)[0].(*lru.Cache[string, *ShortNode])
		targetCache.Add(string(sn.nodeHash[:]), sn)
	} else {
		ssn := SerializeShortNode(sn)
		//将更新后的sn写入db中
		if err := db.Put(sn.nodeHash[:], ssn, nil); err != nil {
			fmt.Println("Insert ShortNode to DB error:", err)
		}
	}
}

// NewFullNode creates a new FullNode and computes its nodeHash
func NewFullNode(children [16]*ShortNode, value []byte, db *leveldb.DB, cache *[]interface{}) *FullNode {
	var nodeHash []byte
	fn := &FullNode{[32]byte{}, nil, children, [16][]byte{}, value, false, make(map[string]map[string]int),
		sync.RWMutex{}, [16]sync.RWMutex{}, sync.Mutex{}}
	for i := 0; i < 16; i++ {
		fn.childLatch[i] = sync.RWMutex{}
	}
	for i := 0; i < 16; i++ {
		if children[i] == nil {
			fn.childrenHash[i] = nil
		} else {
			fn.childrenHash[i] = children[i].nodeHash[:]
			children[i].parent = fn
			nodeHash = append(nodeHash, fn.childrenHash[i][:]...)
		}
	}
	nodeHash = append(nodeHash, value...)
	fn.nodeHash = sha256.Sum256(nodeHash)
	//将fn写入db中
	if cache != nil {
		targetCache, _ := ((*cache)[1]).(*lru.Cache[string, *FullNode])
		targetCache.Add(string(fn.nodeHash[:]), fn)
	} else {
		sfn := SerializeFullNode(fn)
		if err := db.Put(fn.nodeHash[:], sfn, nil); err != nil {
			fmt.Println("Insert FullNode to DB error:", err)
		}
	}
	return fn
}

// UpdateFullNodeHash updates the nodeHash of a FullNode
func (fn *FullNode) UpdateFullNodeHash(db *leveldb.DB, cache *[]interface{}) {
	////先删除db中原有节点
	if cache != nil {
		targetCache, _ := (*cache)[1].(*lru.Cache[string, *FullNode])
		targetCache.Remove(string(fn.nodeHash[:]))
	}
	if err := db.Delete(fn.nodeHash[:], nil); err != nil {
		fmt.Println("Delete FullNode from DB error:", err)
	}
	nodeHash := make([]byte, 0)
	for i := 0; i < 16; i++ {
		if hash_ := fn.childrenHash[i]; hash_ != nil {
			nodeHash = append(nodeHash, hash_[:]...)
		}
	}
	nodeHash = append(nodeHash, fn.value...)
	fn.nodeHash = sha256.Sum256(nodeHash)
	//将更新后的fn写入db中
	if cache != nil {
		targetCache, _ := (*cache)[1].(*lru.Cache[string, *FullNode])
		targetCache.Add(string(fn.nodeHash[:]), fn)
	} else {
		sfn := SerializeFullNode(fn)
		if err := db.Put(fn.nodeHash[:], sfn, nil); err != nil {
			fmt.Println("Insert FullNode to DB error:", err)
		}
	}
}

type SeShortNode struct {
	NodeHash     [32]byte //当前节点的哈希值，由prefix+suffix+value/nextNodeHash计算得到
	Prefix       string   //前缀
	IsLeaf       bool     //是否是叶子节点
	Suffix       string   //后缀，shared nibble（extension node）或key-end（leaf node）
	NextNodeHash [32]byte //下一个FullNode节点的哈希值
	Value        []byte   //value（当前节点是leaf node时）
}

// SerializeShortNode 序列化ShortNode
func SerializeShortNode(sn *ShortNode) []byte {
	ssn := &SeShortNode{sn.GetNodeHash(), sn.GetPrefix(), sn.GetIsLeaf(), sn.GetSuffix(), sn.GetNextNodeHash(), sn.GetValue()}
	if jsonSSN, err := json.Marshal(ssn); err != nil {
		fmt.Printf("SerializeShortNode error: %v\n", err)
		return nil
	} else {
		return jsonSSN
	}
}

// DeserializeShortNode 反序列化ShortNode
func DeserializeShortNode(sSnString []byte) (*ShortNode, error) {
	var ssn SeShortNode
	if err := json.Unmarshal(sSnString, &ssn); err != nil {
		fmt.Printf("DeserializeShortNode error: %v\n", err)
		return nil, err
	}
	sn := &ShortNode{[32]byte{}, ssn.Prefix, nil, ssn.IsLeaf, false, ssn.Suffix, nil,
		[32]byte{}, ssn.Value, make(map[string]map[string]int), sync.RWMutex{}, sync.Mutex{}}
	sn.nodeHash = ssn.NodeHash
	sn.SetNextNodeHash(ssn.NextNodeHash)
	return sn, nil
}

type SeFullNode struct {
	NodeHash     [32]byte   //当前节点的哈希值,由childrenHash计算得到
	ChildrenHash [16][]byte // children的哈希值
	Value        []byte     // 以前面ExtensionNode的prefix+suffix为key的value
}

// SerializeFullNode 序列化FullNode
func SerializeFullNode(fn *FullNode) []byte {
	sfn := &SeFullNode{fn.GetNodeHash(), fn.GetChildrenHash(), fn.GetValue()}
	if jsonSFN, err := json.Marshal(sfn); err != nil {
		fmt.Printf("SerializeFullNode error: %v\n", err)
		return nil
	} else {
		return jsonSFN
	}
}

// DeserializeFullNode 反序列化FullNode
func DeserializeFullNode(sFnString []byte) (*FullNode, error) {
	var sfn SeFullNode
	if err := json.Unmarshal(sFnString, &sfn); err != nil {
		fmt.Printf("DeserializeFullNode error: %v\n", err)
		return nil, err
	}
	var children [16]*ShortNode
	for i := 0; i < 16; i++ {
		children[i] = nil
	}
	fn := &FullNode{sfn.NodeHash, nil, children, sfn.ChildrenHash, sfn.Value, false,
		make(map[string]map[string]int), sync.RWMutex{}, [16]sync.RWMutex{}, sync.Mutex{}}
	for i := 0; i < 16; i++ {
		fn.childLatch[i] = sync.RWMutex{}
	}
	return fn, nil
}

func (sn *ShortNode) GetNodeHash() [32]byte {
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

func (sn *ShortNode) GetNextNodeHash() [32]byte {
	return sn.nextNodeHash
}

func (sn *ShortNode) GetIsLeaf() bool {
	return sn.isLeaf
}

func (sn *ShortNode) SetNextNodeHash(nnh [32]byte) {
	sn.nextNodeHash = nnh
}

func (fn *FullNode) GetNodeHash() [32]byte {
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
	sn.parent = fn
}

func (fn *FullNode) GetValue() []byte {
	return fn.value
}
