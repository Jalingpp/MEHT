package mpt

import (
	"MEHT/util"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"reflect"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

//MPT树相关的结构体和方法
//func (mpt *MPT) GetRoot(db *leveldb.DB) *ShortNode {}：获取MPT的根节点，如果为nil，则从数据库中查询
//func NewMPT() *MPT {}： NewMPT creates an empty MPT
//func (mpt *MPT) Insert(kvPair *util.KVPair, db *leveldb.DB) []byte {}: 插入一个KVPair到MPT中,返回新的根节点的哈希值
//func (mpt *MPT) UpdateMPTInDB(newRootHash []byte, db *leveldb.DB) {}：用newRootHash更新mpt的哈希，并更新至DB中
//func (mpt *MPT) PrintMPT(db *leveldb.DB) {}: 打印MPT
//func (mpt *MPT) QueryByKey(key string, db *leveldb.DB) (string, *MPTProof) {}： 根据key查询value，返回value和证明
//func (mpt *MPT) PrintQueryResult(key string, value string, mptProof *MPTProof) {}: 打印查询结果
//func (mpt *MPT) VerifyQueryResult(value string, mptProof *MPTProof) bool {}: 验证查询结果
//func ComputeMPTRoot(value string, mptProof *MPTProof) []byte {}： 根据MPTProof计算MPT根节点哈希
//func SerializeMPT(mpt *MPT) []byte {}：序列化MPT
//func DeserializeMPT(data []byte) (*MPT, error) {}： 反序列化MPT

type MPT struct {
	rootHash []byte         //MPT的哈希值，对根节点哈希值哈希得到
	root     *ShortNode     //根节点
	cache    *[]interface{} // cache[0], cache[1] represent cache of shortNode and fullNode respectively.
	// the key of any node type is its nodeHash in the form of string
	cacheEnable bool
	latch       sync.RWMutex
	updateLatch sync.Mutex
}

// GetRoot 获取MPT的根节点，如果为nil，则从数据库中查询
func (mpt *MPT) GetRoot(db *leveldb.DB) *ShortNode {
	//如果当前MPT的root为nil，则从数据库中查询
	if mpt.root == nil && mpt.rootHash != nil && mpt.latch.TryLock() { // 只允许一个线程重构mpt树根
		if mpt.root != nil { //防止root在TryLock之前被其他线程重构完毕，导致重复重构
			mpt.latch.Unlock()
			return mpt.root
		}
		if mptRoot, _ := db.Get(mpt.rootHash, nil); len(mptRoot) != 0 {
			mpt.root, _ = DeserializeShortNode(mptRoot)
		}
		mpt.latch.Unlock()
	}
	for mpt.root == nil && mpt.rootHash != nil {
	} // 其余线程等待mpt树根重构
	return mpt.root
}

func (mpt *MPT) GetUpdateLatch() *sync.Mutex {
	return &mpt.updateLatch
}

// NewMPT creates an empty MPT
func NewMPT(db *leveldb.DB, cacheEnable bool, shortNodeCC int, fullNodeCC int) *MPT {
	if cacheEnable {
		lShortNode, _ := lru.NewWithEvict[string, *ShortNode](shortNodeCC, func(k string, v *ShortNode) {
			callBackFoo[string, *ShortNode](k, v, db)
		})
		lFullNode, _ := lru.NewWithEvict[string, *FullNode](fullNodeCC, func(k string, v *FullNode) {
			callBackFoo[string, *FullNode](k, v, db)
		})
		c := make([]interface{}, 0)
		c = append(c, lShortNode, lFullNode)
		return &MPT{nil, nil, &c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	} else {
		return &MPT{nil, nil, nil, cacheEnable, sync.RWMutex{}, sync.Mutex{}}
	}
}

// Insert 插入一个KVPair到MPT中，返回新的根节点的哈希值
func (mpt *MPT) Insert(kvPair util.KVPair, db *leveldb.DB) {
	//判断是否为第一次插入
	for mpt.GetRoot(db) == nil && mpt.latch.TryLock() { // 只允许一个线程新建树根
		if mpt.root != nil { //防止root在TryLock前已经被其他线程创建，导致重复创建
			mpt.latch.Unlock()
			break
		}
		//创建一个ShortNode
		newRoot := NewShortNode("", true, kvPair.GetKey(), nil, []byte(kvPair.GetValue()), db, mpt.cache)
		//更新mpt根哈希并更新到数据库
		mpt.UpdateMPTInDB(newRoot, db)
		mpt.latch.Unlock()
		return
	}
	for mpt.root == nil {
	} // 等待最先拿到mpt锁的线程新建一个树根
	//如果不是第一次插入，递归插入
	oldValueAddedFlag := false //防止读操作在已获得写锁的情况下获取锁
	mpt.latch.Lock()
	newRoot := mpt.RecursiveInsertShortNode("", kvPair.GetKey(), []byte(kvPair.GetValue()), mpt.GetRoot(db), db, &oldValueAddedFlag)
	//更新mpt根哈希并更新到数据库
	mpt.UpdateMPTInDB(newRoot, db)
	mpt.latch.Unlock()
}

// RecursiveInsertShortNode 递归插入当前MPT Node
func (mpt *MPT) RecursiveInsertShortNode(prefix string, suffix string, value []byte, cNode *ShortNode, db *leveldb.DB, flag *bool) *ShortNode {
	//如果当前节点是叶子节点
	cNode.latch.Lock()
	defer cNode.latch.Unlock()
	if flag != nil && !(*flag) { //只在最上层root阶段获取当前视图下mpt存有的value，保证获取的值最新
		val, _ := mpt.QueryByKey(prefix+suffix, db, true)
		toAdd := util.NewKVPair(prefix+suffix, val)
		isChange := toAdd.AddValue(string(value))
		if !isChange { //重复插入，直接返回
			return mpt.root
		}
		value = []byte(toAdd.GetValue())
		*flag = true //其余层节点无需查询最新值
	}
	if cNode.isLeaf {
		//判断当前suffix是否和suffix相同，如果相同，更新value，否则新建一个ExtensionNode，一个BranchNode，一个LeafNode，将两个LeafNode插入到FullNode中
		if strings.Compare(cNode.suffix, suffix) == 0 {
			if !bytes.Equal(cNode.value, value) {
				cNode.value = value
				UpdateShortNodeHash(cNode, db, mpt.cache)
			}
			return cNode
		} else {
			//获取两个suffix的共同前缀
			comPrefix := util.CommPrefix(cNode.suffix, suffix)
			//如果共同前缀的长度等于当前节点的suffix的长度
			if len(comPrefix) == len(cNode.suffix) {
				//新建一个LeafNode（此情况下，suffix一定不为空，前面的判断条件予以保证）
				newLeaf := NewShortNode(prefix+suffix[0:len(comPrefix)+1], true, suffix[len(comPrefix)+1:], nil, value, db, mpt.cache)
				//创建一个BranchNode
				var children [16]*ShortNode
				children[util.ByteToHexIndex(suffix[len(comPrefix)])] = newLeaf
				newBranch := NewFullNode(children, cNode.value, db, mpt.cache)
				//创建一个ExtensionNode，其prefix为之前的prefix，其suffix为comPrefix，其nextNode为新建的FullNode
				newExtension := NewShortNode(prefix, false, comPrefix, newBranch, nil, db, mpt.cache)
				return newExtension
			} else if len(comPrefix) == len(suffix) {
				//如果共同前缀的长度等于suffix的长度
				//更新当前节点的prefix和suffix，nodeHash
				cNode.prefix = cNode.prefix + cNode.suffix[0:len(comPrefix)+1]
				cNode.suffix = cNode.suffix[len(comPrefix)+1:]
				UpdateShortNodeHash(cNode, db, mpt.cache)
				//新建一个FullNode
				var children [16]*ShortNode
				children[util.ByteToHexIndex(cNode.prefix[len(cNode.prefix)-1])] = cNode
				newBranch := NewFullNode(children, value, db, mpt.cache)
				//新建一个ExtensionNode，其prefix为之前的prefix，其suffix为comPrefix，其nextNode为新建的FullNode
				newExtension := NewShortNode(prefix, false, comPrefix, newBranch, nil, db, mpt.cache)
				return newExtension
			} else {
				//新建一个LeafNode,如果suffix在除去comPrefix+1个i字节后没有字节了，则suffix为nil，否则为剩余字节
				var newSuffix1 string
				if len(suffix) > len(comPrefix)+1 {
					newSuffix1 = suffix[len(comPrefix)+1:]
				} else {
					newSuffix1 = ""
				}
				leafNode := NewShortNode(prefix+suffix[0:len(comPrefix)+1], true, newSuffix1, nil, value, db, mpt.cache)
				//更新当前节点的prefix和suffix，nodeHash
				cNode.prefix = cNode.prefix + cNode.suffix[0:len(comPrefix)+1]
				if len(cNode.suffix) > len(comPrefix)+1 {
					cNode.suffix = cNode.suffix[len(comPrefix)+1:]
				} else {
					cNode.suffix = ""
				}
				UpdateShortNodeHash(cNode, db, mpt.cache)
				//创建一个BranchNode
				var children [16]*ShortNode
				children[util.ByteToHexIndex(cNode.prefix[len(cNode.prefix)-1])] = cNode
				children[util.ByteToHexIndex(suffix[len(comPrefix)])] = leafNode
				newBranch := NewFullNode(children, nil, db, mpt.cache)
				//创建一个ExtensionNode，其prefix为之前的prefix，其suffix为comPrefix，其nextNode为新建的FullNode
				newExtension := NewShortNode(prefix, false, comPrefix, newBranch, nil, db, mpt.cache)
				return newExtension
			}
		}
	} else {
		//如果当前节点是ExtensionNode
		//判断当前节点的suffix是否被suffix完全包含，如果可以，递归插入到nextNode中
		//否则新建一个ExtensionNode，一个BranchNode，一个LeafNode
		//将原ExtensionNode和新建的LeafNode插入到FullNode中，FullNode作为新ExtensionNode的nextNode，返回新ExtensionNode
		commPrefix := util.CommPrefix(cNode.suffix, suffix)
		//如果当前节点的suffix被suffix完全包含
		if len(commPrefix) == len(cNode.suffix) {
			//递归插入到nextNode中
			var newSuffix string
			if len(suffix) == len(commPrefix) {
				newSuffix = ""
			} else {
				newSuffix = suffix[len(commPrefix):]
			}
			fullNode := mpt.RecursiveInsertFullNode(prefix+commPrefix, newSuffix, value, cNode.GetNextNode(db, mpt.cache), db)
			cNode.nextNode = fullNode
			cNode.nextNodeHash = fullNode.nodeHash
			UpdateShortNodeHash(cNode, db, mpt.cache)
			return cNode
		} else if len(commPrefix) == len(suffix) {
			//如果当前节点的suffix完全包含suffix
			//更新当前节点的prefix和suffix，nodeHash
			cNode.prefix = cNode.prefix + cNode.suffix[0:len(commPrefix)+1]
			cNode.suffix = cNode.suffix[len(commPrefix)+1:] //当前节点在除去comPrefix后一定还有字节
			UpdateShortNodeHash(cNode, db, mpt.cache)
			//新建一个FullNode，包含当前节点和value
			var children [16]*ShortNode
			children[util.ByteToHexIndex(cNode.prefix[len(cNode.prefix)-1])] = cNode
			newBranch := NewFullNode(children, value, db, mpt.cache)
			//新建一个ExtensionNode，其prefix为之前的prefix，其suffix为comPrefix，其nextNode为新建的FullNode
			newExtension := NewShortNode(prefix, false, commPrefix, newBranch, nil, db, mpt.cache)
			return newExtension
		} else {
			//更新当前节点的prefix和suffix，nodeHash
			cNode.prefix = cNode.prefix + cNode.suffix[0:len(commPrefix)+1]
			if len(cNode.suffix) > len(commPrefix)+1 {
				cNode.suffix = cNode.suffix[len(commPrefix)+1:]
			} else {
				cNode.suffix = ""
			}
			UpdateShortNodeHash(cNode, db, mpt.cache)
			//新建一个LeafNode
			var newSuffix string
			if len(suffix) > len(commPrefix)+1 {
				newSuffix = suffix[len(commPrefix)+1:]
			} else {
				newSuffix = ""
			}
			newLeaf := NewShortNode(prefix+suffix[0:len(commPrefix)+1], true, newSuffix, nil, value, db, mpt.cache)
			//创建一个BranchNode
			var children [16]*ShortNode
			children[util.ByteToHexIndex(cNode.prefix[len(cNode.prefix)-1])] = cNode
			children[util.ByteToHexIndex(suffix[len(commPrefix)])] = newLeaf
			newBranch := NewFullNode(children, nil, db, mpt.cache)
			//创建一个ExtensionNode，其prefix为之前的prefix，其suffix为comPrefix，其nextNode为一个FullNode
			newExtension := NewShortNode(prefix, false, commPrefix, newBranch, nil, db, mpt.cache)
			return newExtension
		}
	}
}

func (mpt *MPT) RecursiveInsertFullNode(prefix string, suffix string, value []byte, cNode *FullNode, db *leveldb.DB) *FullNode {
	//如果当前节点是FullNode
	cNode.latch.Lock()
	defer cNode.latch.Unlock()
	//如果len(suffix)==0，则value插入到当前FullNode的value中；否则，递归插入到children中
	if len(suffix) == 0 {
		if !bytes.Equal(cNode.value, value) {
			cNode.value = value
			UpdateFullNodeHash(cNode, db, mpt.cache)
		}
		return cNode
	} else {
		var childNode_ *ShortNode                                                            //新创建的childNode或递归查询返回的childNode
		childNode := cNode.GetChildInFullNode(util.ByteToHexIndex(suffix[0]), db, mpt.cache) //当前fullNode中已有的childNode
		var newSuffix string
		if len(suffix) > 1 {
			newSuffix = suffix[1:]
		} else {
			newSuffix = ""
		}
		if childNode != nil {
			childNode_ = mpt.RecursiveInsertShortNode(prefix+suffix[:1], newSuffix, value, childNode, db, nil)
		} else {
			childNode_ = NewShortNode(prefix+suffix[:1], true, newSuffix, nil, value, db, mpt.cache)
		}
		cNode.children[util.ByteToHexIndex(suffix[0])] = childNode_
		cNode.childrenHash[util.ByteToHexIndex(suffix[0])] = childNode_.nodeHash
		UpdateFullNodeHash(cNode, db, mpt.cache)
		return cNode
	}
}

// UpdateMPTInDB 用newRootHash更新mpt的哈希，并更新至DB中
func (mpt *MPT) UpdateMPTInDB(newRoot *ShortNode, db *leveldb.DB) {
	//DB 中索引MPT的是其根哈希的哈希
	mpt.updateLatch.Lock()
	defer mpt.updateLatch.Unlock()
	hash := sha256.Sum256(mpt.rootHash)
	mptHash := hash[:]
	if len(mptHash) > 0 {
		//删除db中原有的MPT
		if err := db.Delete(mptHash, nil); err != nil {
			fmt.Println("Delete MPT from DB error:", err)
		}
	}
	//更新mpt的root与rootHash
	mpt.root = newRoot
	mpt.rootHash = newRoot.nodeHash
	//计算新的mptHash
	hash = sha256.Sum256(mpt.rootHash)
	mptHash = hash[:]
	//将更新后的mpt写入db中
	if err := db.Put(mptHash, SerializeMPT(mpt), nil); err != nil {
		fmt.Println("Insert MPT to DB error:", err)
	}
}

func (mpt *MPT) PurgeCache() {
	for idx, cache_ := range *(mpt.cache) {
		switch idx {
		case 0:
			targetCache, _ := cache_.(*lru.Cache[string, *ShortNode])
			targetCache.Purge()
		case 1:
			targetCache, _ := cache_.(*lru.Cache[string, *FullNode])
			targetCache.Purge()
		default:
			panic("Unknown idx of mptCache with type " + reflect.TypeOf(cache_).String() + ".")
		}
	}
}

func callBackFoo[K comparable, V any](k K, v V, db *leveldb.DB) {
	k_, err := util.ToStringE(k)
	if err != nil {
		panic(err)
	}
	var v_ []byte
	switch any(v).(type) {
	case *ShortNode:
		v_ = SerializeShortNode(any(v).(*ShortNode))
	case *FullNode:
		v_ = SerializeFullNode(any(v).(*FullNode))
	default:
		panic("Unknown type " + reflect.TypeOf(v).String() + " in callBackFoo of MPT.")
	}
	if err = db.Put([]byte(k_), v_, nil); err != nil {
		panic(err)
	}
}

// PrintMPT 打印MPT
func (mpt *MPT) PrintMPT(db *leveldb.DB) {
	root := mpt.GetRoot(db)
	if root == nil {
		return
	}
	mpt.RecursivePrintShortNode(root, 0, db)
}

// RecursivePrintShortNode 递归打印ShortNode
func (mpt *MPT) RecursivePrintShortNode(cNode *ShortNode, level int, db *leveldb.DB) {
	//如果当前节点是叶子节点
	if cNode.isLeaf {
		//打印当前叶子节点
		fmt.Printf("level: %d, leafNode:%x\n", level, cNode.nodeHash)
		fmt.Printf("prefix:%s, suffix:%s, value:%s\n", cNode.prefix, cNode.suffix, string(cNode.value))
	} else {
		//打印当前Extension节点
		fmt.Printf("level: %d, extensionNode:%x\n", level, cNode.nodeHash)
		fmt.Printf("prefix:%s, suffix:%s, next node:%x\n", cNode.prefix, cNode.suffix, cNode.nextNodeHash)
		//递归打印nextNode
		mpt.RecursivePrintFullNode(cNode.GetNextNode(db, mpt.cache), level+1, db)
	}
}

// RecursivePrintFullNode 递归打印FullNode
func (mpt *MPT) RecursivePrintFullNode(cNode *FullNode, level int, db *leveldb.DB) {
	//打印当前FullNode
	fmt.Printf("level: %d, fullNode:%x, value:%s\n", level, cNode.nodeHash, string(cNode.value))
	//打印所有孩子节点的hash
	for i := 0; i < 16; i++ {
		if cNode.childrenHash[i] != nil {
			fmt.Printf("children[%d]:%x\n", i, cNode.childrenHash[i])
		}
	}
	//递归打印所有孩子节点
	for i := 0; i < 16; i++ {
		childNode := cNode.GetChildInFullNode(i, db, mpt.cache)
		if childNode != nil {
			// fmt.Printf("已获取childNode[%d]:%x\n", i, childNode.nodeHash)
			mpt.RecursivePrintShortNode(childNode, level+1, db)
		}
		// else {
		// 	fmt.Printf("childNode[%d]不存在\n", i)
		// }
	}
}

// QueryByKey 根据key查询value，返回value和证明
func (mpt *MPT) QueryByKey(key string, db *leveldb.DB, isLockFree bool) (string, *MPTProof) {
	//如果MPT为空，返回空
	if root := mpt.GetRoot(db); root == nil {
		return "", &MPTProof{false, 0, nil}
	} else {
		//递归查询
		return mpt.RecursiveQueryShortNode(key, 0, 0, root, db, isLockFree)
	}
}

func (mpt *MPT) RecursiveQueryShortNode(key string, p int, level int, cNode *ShortNode, db *leveldb.DB, isLockFree bool) (string, *MPTProof) {
	if cNode == nil {
		return "", &MPTProof{false, 0, nil}
	}
	if !isLockFree {
		cNode.latch.RLock()
		defer cNode.latch.RUnlock()
	}
	//当前节点是叶子节点
	if cNode.isLeaf {
		//构造当前节点的证明
		proofElement := NewProofElement(level, 0, cNode.prefix, cNode.suffix, cNode.value, nil, [16][]byte{})
		//找到对应key的value
		if strings.Compare(cNode.suffix, key[p:]) == 0 {
			return string(cNode.value), &MPTProof{true, level, []*ProofElement{proofElement}}
		} else {
			return "", &MPTProof{false, level, []*ProofElement{proofElement}}
		}
	} else {
		//当前节点是ExtensionNode
		//构造当前节点的证明
		proofElement := NewProofElement(level, 1, cNode.prefix, cNode.suffix, nil, cNode.nextNodeHash, [16][]byte{})
		//当前节点的suffix被key的suffix完全包含，则继续递归查询nextNode，将子查询结果与当前结果合并返回
		if cNode.suffix == "" || p < len(key) && len(util.CommPrefix(cNode.suffix, key[p:])) == len(cNode.suffix) {
			nextNode := cNode.GetNextNode(db, mpt.cache)
			valueStr, mptProof := mpt.RecursiveQueryFullNode(key, p+len(cNode.suffix), level+1, nextNode, db, isLockFree)
			proofElements := append(mptProof.GetProofs(), proofElement)
			return valueStr, &MPTProof{mptProof.GetIsExist(), mptProof.GetLevels(), proofElements}
		} else {
			return "", &MPTProof{false, level, []*ProofElement{proofElement}}
		}
	}
}

func (mpt *MPT) RecursiveQueryFullNode(key string, p int, level int, cNode *FullNode, db *leveldb.DB, isLockFree bool) (string, *MPTProof) {
	if !isLockFree {
		cNode.latch.RLock()
		defer cNode.latch.RUnlock()
	}
	proofElement := NewProofElement(level, 2, "", "", cNode.value, nil, cNode.childrenHash)
	if p >= len(key) {
		//判断当前FullNode是否有value，如果有，构造存在证明返回
		if cNode.value != nil {
			return string(cNode.value), &MPTProof{true, level, []*ProofElement{proofElement}}
		} else {
			return "", &MPTProof{false, level, []*ProofElement{proofElement}}
		}
	}
	//如果当前FullNode的children中没有对应的key[p]，则构造不存在证明返回
	childNodeP := cNode.GetChildInFullNode(util.ByteToHexIndex(key[p]), db, mpt.cache)
	if childNodeP == nil {
		return "", &MPTProof{false, level, []*ProofElement{proofElement}}
	}
	//如果当前FullNode的children中有对应的key[p]，则递归查询children，将子查询结果与当前结果合并返回
	valueStr, mptProof := mpt.RecursiveQueryShortNode(key, p+1, level+1, childNodeP, db, isLockFree)
	proofElements := append(mptProof.GetProofs(), proofElement)
	return valueStr, &MPTProof{mptProof.GetIsExist(), mptProof.GetLevels(), proofElements}
}

// PrintQueryResult 打印查询结果
func (mpt *MPT) PrintQueryResult(key string, value string, mptProof *MPTProof) {
	fmt.Printf("查询结果-------------------------------------------------------------------------------------------\n")
	fmt.Printf("key=%s\n", key)
	if value == "" {
		fmt.Printf("value不存在\n")
	} else {
		fmt.Printf("value=%s\n", value)
	}
	mptProof.PrintMPTProof()
}

// VerifyQueryResult 验证查询结果
func (mpt *MPT) VerifyQueryResult(value string, mptProof *MPTProof) bool {
	computedMPTRoot := ComputeMPTRoot(value, mptProof)
	if !bytes.Equal(computedMPTRoot, mpt.root.nodeHash) {
		fmt.Printf("根哈希值%x计算错误,验证不通过\n", computedMPTRoot)
		return false
	}
	fmt.Printf("根哈希值%x计算正确,验证通过\n", computedMPTRoot)
	return true
}

// ComputeMPTRoot 根据MPTProof计算MPT根节点哈希
func ComputeMPTRoot(value string, mptProof *MPTProof) []byte {
	proofs := mptProof.GetProofs()
	nodeHash0 := []byte(value)
	nodeHash1 := make([]byte, 0)
	for i := 0; i < len(proofs); i++ {
		proof := proofs[i]
		if proof.proofType == 0 {
			nodeHash1 = append([]byte(proof.prefix), proof.suffix...)
			//如果存在，则用查询得到的value计算，否则用proof的value计算
			if mptProof.isExist {
				nodeHash1 = append(nodeHash1, []byte(value)...)
			} else {
				nodeHash1 = append(nodeHash1, proof.value...)
			}
			hash := sha256.Sum256(nodeHash1)
			nodeHash0 = hash[:]
			nodeHash1 = nil
		} else if proof.proofType == 1 {
			//如果当前proof不是最底层，则验证下层子树的根是否在当前层
			if proof.level != mptProof.levels {
				if !bytes.Equal(proof.nextNodeHash, nodeHash0) {
					fmt.Printf("level %d nextNodeHash=%x计算错误,验证不通过\n", proof.level, nodeHash0)
					return nil
				}
			}
			nodeHash1 = append([]byte(proof.prefix), proof.suffix...)
			nodeHash1 = append(nodeHash1, proof.nextNodeHash...)
			hash := sha256.Sum256(nodeHash1)
			nodeHash0 = hash[:]
			nodeHash1 = nil
		} else {
			//如果当前proof不是最底层，则验证下层子树的根是否在当前层
			if proof.level != mptProof.levels {
				isIn := false
				for i := 0; i < 16; i++ {
					if bytes.Equal(proof.childrenHashes[i], nodeHash0) {
						isIn = true
						break
					}
				}
				if !isIn {
					fmt.Printf("level %d childrenHashes=%x计算错误,验证不通过\n", proof.level, nodeHash0)
					return nil
				}
			}
			for i := 0; i < 16; i++ {
				nodeHash1 = append(nodeHash1, proof.childrenHashes[i]...)
			}
			nodeHash1 = append(nodeHash1, proof.value...)
			hash := sha256.Sum256(nodeHash1)
			nodeHash0 = hash[:]
			nodeHash1 = nil
		}
	}
	return nodeHash0
}

type SeMPT struct {
	RootHash []byte //MPT的哈希值，对根节点哈希值哈希得到
}

// SerializeMPT 序列化MPT
func SerializeMPT(mpt *MPT) []byte {
	sMpt := &SeMPT{mpt.rootHash}
	jsonSSN, err := json.Marshal(sMpt)
	if err != nil {
		fmt.Printf("SerializeMPT error: %v\n", err)
		return nil
	}
	return jsonSSN
}

// DeserializeMPT 反序列化MPT
func DeserializeMPT(data []byte, db *leveldb.DB, cacheEnable bool, shortNodeCC int, fullNodeCC int) (*MPT, error) {
	var sMpt SeMPT
	if err := json.Unmarshal(data, &sMpt); err != nil {
		fmt.Printf("DeserializeMPT error: %v\n", err)
		return nil, err
	}
	if cacheEnable {
		lShortNode, _ := lru.NewWithEvict(shortNodeCC, func(k string, v *ShortNode) {
			callBackFoo[string, *ShortNode](k, v, db)
		})
		lFullNode, _ := lru.NewWithEvict(fullNodeCC, func(k string, v *FullNode) {
			callBackFoo[string, *FullNode](k, v, db)
		})
		var c []interface{}
		c = append(c, lShortNode, lFullNode)
		return &MPT{sMpt.RootHash, nil, &c, cacheEnable, sync.RWMutex{}, sync.Mutex{}}, nil
	} else {
		return &MPT{sMpt.RootHash, nil, nil, cacheEnable, sync.RWMutex{}, sync.Mutex{}}, nil
	}
}

func (mpt *MPT) GetRootHash() []byte {
	return mpt.rootHash
}
