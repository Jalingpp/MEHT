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

	"github.com/syndtr/goleveldb/leveldb"
)

//MPT树相关的结构体和方法
//func (mpt *MPT) GetRoot(db *leveldb.DB) *ShortNode {}：获取MPT的根节点，如果为nil，则从数据库中查询
//func NewMPT() *MPT {}： NewMPT creates a empty MPT
//func (mpt *MPT) Insert(kvpair *util.KVPair, db *leveldb.DB) []byte {}: 插入一个KVPair到MPT中,返回新的根节点的哈希值
//func (mpt *MPT) UpdateMPTInDB(newRootHash []byte, db *leveldb.DB) {}：用newRootHash更新mpt的哈希，并更新至DB中
//func (mpt *MPT) PrintMPT(db *leveldb.DB) {}: 打印MPT
//func (mpt *MPT) QueryByKey(key string, db *leveldb.DB) (string, *MPTProof) {}： 根据key查询value，返回value和证明
//func (mpt *MPT) PrintQueryResult(key string, value string, mptProof *MPTProof) {}: 打印查询结果
//func (mpt *MPT) VerifyQueryResult(value string, mptProof *MPTProof) bool {}: 验证查询结果
//func ComputMPTRoot(value string, mptProof *MPTProof) []byte {}： 根据MPTProof计算MPT根节点哈希
//func SerializeMPT(mpt *MPT) []byte {}：序列化MPT
//func DeserializeMPT(data []byte) (*MPT, error) {}： 反序列化MPT

type MPT struct {
	rootHash []byte         //MPT的哈希值，对根节点哈希值哈希得到
	root     *ShortNode     //根节点
	cache    *[]interface{} // cache[0], cache[1] represent cache of shortNode and fullNode respectively.
	// the key of any node type is its nodeHash in the form of string
	cacheEnable bool
}

// 获取MPT的根节点，如果为nil，则从数据库中查询
func (mpt *MPT) GetRoot(db *leveldb.DB) *ShortNode {
	//如果当前MPT的root为nil，则从数据库中查询
	if mpt.root == nil && mpt.rootHash != nil {
		if mptRoot, _ := db.Get(mpt.rootHash, nil); len(mptRoot) != 0 {
			mpt.root, _ = DeserializeShortNode(mptRoot)
		}
	}
	return mpt.root
}

// NewMPT creates a empty MPT
func NewMPT(db *leveldb.DB, cacheEnable bool, shortNodeCC int, fullNodeCC int) *MPT {
	if cacheEnable {
		lShortNode, _ := lru.NewWithEvict[string, *ShortNode](shortNodeCC, func(k string, v *ShortNode) {
			callBackFoo[string, *ShortNode](k, v, db)
		})
		lFullNode, _ := lru.NewWithEvict[string, *FullNode](fullNodeCC, func(k string, v *FullNode) {
			callBackFoo[string, *FullNode](k, v, db)
		})
		var c []interface{}
		c = append(c, lShortNode, lFullNode)
		return &MPT{nil, nil, &c, cacheEnable}
	} else {
		return &MPT{nil, nil, nil, cacheEnable}
	}
}

// 插入一个KVPair到MPT中，返回新的根节点的哈希值
func (mpt *MPT) Insert(kvpair *util.KVPair, db *leveldb.DB) []byte {
	//判断是否为第一次插入
	if mpt.GetRoot(db) == nil {
		//创建一个ShortNode
		mpt.root = NewShortNode("", true, kvpair.GetKey(), nil, []byte(kvpair.GetValue()), db)
		//更新mpt根哈希并更新到数据库
		mpt.UpdateMPTInDB(mpt.root.nodeHash, db)
		return mpt.rootHash
	}
	//如果不是第一次插入，递归插入
	mpt.root = mpt.RecursiveInsertShortNode("", kvpair.GetKey(), []byte(kvpair.GetValue()), mpt.GetRoot(db), db)
	//更新mpt根哈希并更新到数据库
	mpt.UpdateMPTInDB(mpt.root.nodeHash, db)
	return mpt.rootHash
}

// 递归插入当前MPT Node
func (mpt *MPT) RecursiveInsertShortNode(prefix string, suffix string, value []byte, cnode *ShortNode, db *leveldb.DB) *ShortNode {
	//如果当前节点是叶子节点
	if cnode.isLeaf {
		//判断当前suffix是否和suffix相同，如果相同，更新value，否则新建一个ExtensionNode，一个BranchNode，一个LeafNode，将两个LeafNode插入到FullNode中
		if strings.Compare(cnode.suffix, suffix) == 0 {
			if !bytes.Equal(cnode.value, value) {
				cnode.value = value
				UpdateShortNodeHash(cnode, db, mpt.cache)
			}
			return cnode
		} else {
			//获取两个suffix的共同前缀
			comprefix := util.CommPrefix(cnode.suffix, suffix)
			//如果共同前缀的长度等于当前节点的suffix的长度
			if len(comprefix) == len(cnode.suffix) {
				//新建一个LeafNode（此情况下，suffix一定不为空，前面的判断条件予以保证）
				newLeaf := NewShortNode(prefix+suffix[0:len(comprefix)+1], true, suffix[len(comprefix)+1:], nil, value, db)
				//创建一个BranchNode
				var children [16]*ShortNode
				children[util.ByteToHexIndex(suffix[len(comprefix)])] = newLeaf
				newBranch := NewFullNode(children, cnode.value, db)
				//创建一个ExtensionNode，其prefix为之前的prefix，其suffix为comprefix，其nextNode为新建的FullNode
				newExtension := NewShortNode(prefix, false, comprefix, newBranch, nil, db)
				return newExtension
			} else if len(comprefix) == len(suffix) {
				//如果共同前缀的长度等于suffix的长度
				//更新当前节点的prefix和suffix，nodeHash
				cnode.prefix = cnode.prefix + cnode.suffix[0:len(comprefix)+1]
				cnode.suffix = cnode.suffix[len(comprefix)+1:]
				UpdateShortNodeHash(cnode, db, mpt.cache)
				//新建一个FullNode
				var children [16]*ShortNode
				children[util.ByteToHexIndex(cnode.prefix[len(cnode.prefix)-1])] = cnode
				newBranch := NewFullNode(children, value, db)
				//新建一个ExtensionNode，其prefix为之前的prefix，其suffix为comprefix，其nextNode为新建的FullNode
				newExtension := NewShortNode(prefix, false, comprefix, newBranch, nil, db)
				return newExtension
			} else {
				//新建一个LeafNode,如果suffix在除去comprefix+1个i字节后没有字节了，则suffix为nil，否则为剩余字节
				var newsuffix1 string
				if len(suffix) > len(comprefix)+1 {
					newsuffix1 = suffix[len(comprefix)+1:]
				} else {
					newsuffix1 = ""
				}
				leafnode := NewShortNode(prefix+suffix[0:len(comprefix)+1], true, newsuffix1, nil, value, db)
				//更新当前节点的prefix和suffix，nodeHash
				cnode.prefix = cnode.prefix + cnode.suffix[0:len(comprefix)+1]
				if len(cnode.suffix) > len(comprefix)+1 {
					cnode.suffix = cnode.suffix[len(comprefix)+1:]
				} else {
					cnode.suffix = ""
				}
				UpdateShortNodeHash(cnode, db, mpt.cache)
				//创建一个BranchNode
				var children [16]*ShortNode
				children[util.ByteToHexIndex(cnode.prefix[len(cnode.prefix)-1])] = cnode
				children[util.ByteToHexIndex(suffix[len(comprefix)])] = leafnode
				newBranch := NewFullNode(children, nil, db)
				//创建一个ExtensionNode，其prefix为之前的prefix，其suffix为comprefix，其nextNode为新建的FullNode
				newExtension := NewShortNode(prefix, false, comprefix, newBranch, nil, db)
				return newExtension
			}
		}
	} else {
		//如果当前节点是ExtensionNode
		//判断当前节点的suffix是否被suffix完全包含，如果可以，递归插入到nextNode中
		//否则新建一个ExtensionNode，一个BranchNode，一个LeafNode
		//将原ExrensionNode和新建的LeafNode插入到FullNode中，FullNode作为新ExtensionNode的nextNode，返回新ExtensionNode
		commPrefix := util.CommPrefix(cnode.suffix, suffix)
		//如果当前节点的suffix被suffix完全包含
		if len(commPrefix) == len(cnode.suffix) {
			//递归插入到nextNode中
			var newsuffix string
			if len(suffix) == len(commPrefix) {
				newsuffix = ""
			} else {
				newsuffix = suffix[len(commPrefix):]
			}
			fullnode := mpt.RecursiveInsertFullNode(prefix+commPrefix, newsuffix, value, cnode.GetNextNode(db, mpt.cache), db)
			cnode.nextNode = fullnode
			cnode.nextNodeHash = fullnode.nodeHash
			UpdateShortNodeHash(cnode, db, mpt.cache)
			return cnode
		} else if len(commPrefix) == len(suffix) {
			//如果当前节点的suffix完全包含suffix
			//更新当前节点的prefix和suffix，nodeHash
			cnode.prefix = cnode.prefix + cnode.suffix[0:len(commPrefix)+1]
			cnode.suffix = cnode.suffix[len(commPrefix)+1:] //当前节点在除去comfix后一定还有字节
			UpdateShortNodeHash(cnode, db, mpt.cache)
			//新建一个FullNode，包含当前节点和value
			var children [16]*ShortNode
			children[util.ByteToHexIndex(cnode.prefix[len(cnode.prefix)-1])] = cnode
			newBranch := NewFullNode(children, value, db)
			//新建一个ExtensionNode，其prefix为之前的prefix，其suffix为comprefix，其nextNode为新建的FullNode
			newExtension := NewShortNode(prefix, false, commPrefix, newBranch, nil, db)
			return newExtension
		} else {
			//更新当前节点的prefix和suffix，nodeHash
			cnode.prefix = cnode.prefix + cnode.suffix[0:len(commPrefix)+1]
			if len(cnode.suffix) > len(commPrefix)+1 {
				cnode.suffix = cnode.suffix[len(commPrefix)+1:]
			} else {
				cnode.suffix = ""
			}
			UpdateShortNodeHash(cnode, db, mpt.cache)
			//新建一个LeafNode
			var newsuffix string
			if len(suffix) > len(commPrefix)+1 {
				newsuffix = suffix[len(commPrefix)+1:]
			} else {
				newsuffix = ""
			}
			newLeaf := NewShortNode(prefix+suffix[0:len(commPrefix)+1], true, newsuffix, nil, value, db)
			//创建一个BranchNode
			var children [16]*ShortNode
			children[util.ByteToHexIndex(cnode.prefix[len(cnode.prefix)-1])] = cnode
			children[util.ByteToHexIndex(suffix[len(commPrefix)])] = newLeaf
			newBranch := NewFullNode(children, nil, db)
			//创建一个ExtensionNode，其prefix为之前的prefix，其suffix为comprefix，其nextNode为一个FullNode
			newExtension := NewShortNode(prefix, false, commPrefix, newBranch, nil, db)
			return newExtension
		}
	}
}

func (mpt *MPT) RecursiveInsertFullNode(prefix string, suffix string, value []byte, cnode *FullNode, db *leveldb.DB) *FullNode {
	//如果当前节点是FullNode
	//如果len(suffix)==0，则value插入到当前FullNode的value中；否则，递归插入到children中
	if len(suffix) == 0 {
		if !bytes.Equal(cnode.value, value) {
			cnode.value = value
			UpdateFullNodeHash(cnode, db, mpt.cache)
		}
		return cnode
	} else {
		var childnode *ShortNode                                                             //新创建的childNode或递归查询返回的childNode
		childNode := cnode.GetChildInFullNode(util.ByteToHexIndex(suffix[0]), db, mpt.cache) //当前fullnode中已有的childNode
		var newsuffix string
		if len(suffix) > 1 {
			newsuffix = suffix[1:]
		} else {
			newsuffix = ""
		}
		if childNode != nil {
			childnode = mpt.RecursiveInsertShortNode(prefix+suffix[:1], newsuffix, value, childNode, db)
		} else {
			childnode = NewShortNode(prefix+suffix[:1], true, newsuffix, nil, value, db)
		}
		cnode.children[util.ByteToHexIndex(suffix[0])] = childnode
		cnode.childrenHash[util.ByteToHexIndex(suffix[0])] = childnode.nodeHash
		UpdateFullNodeHash(cnode, db, mpt.cache)
		return cnode
	}
}

// 用newRootHash更新mpt的哈希，并更新至DB中
func (mpt *MPT) UpdateMPTInDB(newRootHash []byte, db *leveldb.DB) {
	//DB 中索引MPT的是其根哈希的哈希
	hashs := sha256.Sum256(mpt.rootHash)
	mptHash := hashs[:]
	if len(mptHash) > 0 {
		//删除db中原有的MPT
		if err := db.Delete(mptHash, nil); err != nil {
			fmt.Println("Delete MPT from DB error:", err)
		}
	}
	//更新mpt的rootHash
	mpt.rootHash = newRootHash
	//计算新的mptHash
	hashs = sha256.Sum256(mpt.rootHash)
	mptHash = hashs[:]
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

// 打印MPT
func (mpt *MPT) PrintMPT(db *leveldb.DB) {
	root := mpt.GetRoot(db)
	if root == nil {
		return
	}
	mpt.RecursivePrintShortNode(root, 0, db)
}

// 递归打印ShortNode
func (mpt *MPT) RecursivePrintShortNode(cnode *ShortNode, level int, db *leveldb.DB) {
	//如果当前节点是叶子节点
	if cnode.isLeaf {
		//打印当前叶子节点
		fmt.Printf("level: %d, leafNode:%x\n", level, cnode.nodeHash)
		fmt.Printf("prefix:%s, suffix:%s, value:%s\n", cnode.prefix, cnode.suffix, string(cnode.value))
	} else {
		//打印当前Extension节点
		fmt.Printf("level: %d, extensionNode:%x\n", level, cnode.nodeHash)
		fmt.Printf("prefix:%s, suffix:%s, next node:%x\n", cnode.prefix, cnode.suffix, cnode.nextNodeHash)
		//递归打印nextNode
		mpt.RecursivePrintFullNode(cnode.GetNextNode(db, mpt.cache), level+1, db)
	}
}

// 递归打印FullNode
func (mpt *MPT) RecursivePrintFullNode(cnode *FullNode, level int, db *leveldb.DB) {
	//打印当前FullNode
	fmt.Printf("level: %d, fullNode:%x, value:%s\n", level, cnode.nodeHash, string(cnode.value))
	//打印所有孩子节点的hash
	for i := 0; i < 16; i++ {
		if cnode.childrenHash[i] != nil {
			fmt.Printf("children[%d]:%x\n", i, cnode.childrenHash[i])
		}
	}
	//递归打印所有孩子节点
	for i := 0; i < 16; i++ {
		childNode := cnode.GetChildInFullNode(i, db, mpt.cache)
		if childNode != nil {
			// fmt.Printf("已获取childNode[%d]:%x\n", i, childNode.nodeHash)
			mpt.RecursivePrintShortNode(childNode, level+1, db)
		}
		// else {
		// 	fmt.Printf("childNode[%d]不存在\n", i)
		// }
	}
}

// 根据key查询value，返回value和证明
func (mpt *MPT) QueryByKey(key string, db *leveldb.DB) (string, *MPTProof) {
	//如果MPT为空，返回空
	root := mpt.GetRoot(db)
	if root == nil {
		return "", &MPTProof{false, 0, nil}
	}
	//递归查询
	return mpt.RecursiveQueryShortNode(key, 0, 0, root, db)
}

func (mpt *MPT) RecursiveQueryShortNode(key string, p int, level int, cnode *ShortNode, db *leveldb.DB) (string, *MPTProof) {
	if cnode == nil {
		return "", &MPTProof{false, 0, nil}
	}
	//当前节点是叶子节点
	if cnode.isLeaf {
		//构造当前节点的证明
		proofElement := NewProofElement(level, 0, cnode.prefix, cnode.suffix, cnode.value, nil, [16][]byte{})
		//找到对应key的value
		if strings.Compare(cnode.suffix, key[p:]) == 0 {
			return string(cnode.value), &MPTProof{true, level, []*ProofElement{proofElement}}
		} else {
			return "", &MPTProof{false, level, []*ProofElement{proofElement}}
		}
	} else {
		//当前节点是ExtensionNode
		//构造当前节点的证明
		proofElement := NewProofElement(level, 1, cnode.prefix, cnode.suffix, nil, cnode.nextNodeHash, [16][]byte{})
		//当前节点的suffix被key的suffix完全包含，则继续递归查询nextNode，将子查询结果与当前结果合并返回
		if cnode.suffix == "" || p < len(key) && len(util.CommPrefix(cnode.suffix, key[p:])) == len(cnode.suffix) {
			nextNode := cnode.GetNextNode(db, mpt.cache)
			valuestr, mptProof := mpt.RecursiveQueryFullNode(key, p+len(cnode.suffix), level+1, nextNode, db)
			proofElements := append(mptProof.GetProofs(), proofElement)
			return valuestr, &MPTProof{mptProof.GetIsExist(), mptProof.GetLevels(), proofElements}
		} else {
			return "", &MPTProof{false, level, []*ProofElement{proofElement}}
		}
	}
}

func (mpt *MPT) RecursiveQueryFullNode(key string, p int, level int, cnode *FullNode, db *leveldb.DB) (string, *MPTProof) {
	proofElement := NewProofElement(level, 2, "", "", cnode.value, nil, cnode.childrenHash)
	if p >= len(key) {
		//判断当前FullNode是否有value，如果有，构造存在证明返回
		if cnode.value != nil {
			return string(cnode.value), &MPTProof{true, level, []*ProofElement{proofElement}}
		} else {
			return "", &MPTProof{false, level, []*ProofElement{proofElement}}
		}
	}
	//如果当前FullNode的children中没有对应的key[p]，则构造不存在证明返回
	childNodeP := cnode.GetChildInFullNode(util.ByteToHexIndex(key[p]), db, mpt.cache)
	if childNodeP == nil {
		return "", &MPTProof{false, level, []*ProofElement{proofElement}}
	}
	//如果当前FullNode的children中有对应的key[p]，则递归查询children，将子查询结果与当前结果合并返回
	valuestr, mptProof := mpt.RecursiveQueryShortNode(key, p+1, level+1, childNodeP, db)
	proofElements := append(mptProof.GetProofs(), proofElement)
	return valuestr, &MPTProof{mptProof.GetIsExist(), mptProof.GetLevels(), proofElements}
}

// 打印查询结果
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

// 验证查询结果
func (mpt *MPT) VerifyQueryResult(value string, mptProof *MPTProof) bool {
	computedMPTRoot := ComputMPTRoot(value, mptProof)
	if !bytes.Equal(computedMPTRoot, mpt.root.nodeHash) {
		fmt.Printf("根哈希值%x计算错误,验证不通过\n", computedMPTRoot)
		return false
	}
	fmt.Printf("根哈希值%x计算正确,验证通过\n", computedMPTRoot)
	return true
}

// 根据MPTProof计算MPT根节点哈希
func ComputMPTRoot(value string, mptProof *MPTProof) []byte {
	proofs := mptProof.GetProofs()
	nodeHash0 := []byte(value)
	var nodeHash1 []byte
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

// 序列化MPT
func SerializeMPT(mpt *MPT) []byte {
	smpt := &SeMPT{mpt.rootHash}
	jsonSSN, err := json.Marshal(smpt)
	if err != nil {
		fmt.Printf("SerializeMPT error: %v\n", err)
		return nil
	}
	return jsonSSN
}

// 反序列化MPT
func DeserializeMPT(data []byte, db *leveldb.DB, cacheEnable bool, shortNodeCC int, fullNodeCC int) (*MPT, error) {
	var smpt SeMPT
	if err := json.Unmarshal(data, &smpt); err != nil {
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
		return &MPT{smpt.RootHash, nil, &c, cacheEnable}, nil
	} else {
		return &MPT{smpt.RootHash, nil, nil, cacheEnable}, nil
	}
}

func (mpt *MPT) GetRootHash() []byte {
	return mpt.rootHash
}

// func main() {
// 	//测试MPT insert
// 	mpt := mpt.NewMPT()

// 	kvpair1 := util.NewKVPair("a711355", "value1")
// 	kvpair2 := util.NewKVPair("a77d337", "value2")
// 	kvpair3 := util.NewKVPair("a7f9365", "value3")
// 	kvpair4 := util.NewKVPair("a77d397", "value4")

// 	//测试MPT insert
// 	mpt.Insert(kvpair1)
// 	mpt.Insert(kvpair2)
// 	mpt.Insert(kvpair3)
// 	mpt.Insert(kvpair4)

// 	// kvpair4 = util.NewKVPair("a77d397", "value5")

// 	// fmt.Printf("key1:%x\n", []byte(kvpair1.GetKey()))
// 	// fmt.Printf("value1:%x\n", []byte(kvpair1.GetValue()))
// 	// fmt.Printf("key2:%x\n", []byte(kvpair2.GetKey()))
// 	// fmt.Printf("value2:%x\n", []byte(kvpair2.GetValue()))
// 	// fmt.Printf("key3:%x\n", []byte(kvpair3.GetKey()))
// 	// fmt.Printf("value3:%x\n", []byte(kvpair3.GetValue()))
// 	// fmt.Printf("key4:%x\n", []byte(kvpair4.GetKey()))
// 	// fmt.Printf("value4:%x\n", []byte(kvpair4.GetValue()))
// 	// mpt.Insert(*kvpair4)

// 	//测试MPT print
// 	mpt.PrintMPT()

// 	//测试MPT query
// 	// qv1, qpf1 := mpt.QueryByKey(kvpair1.GetKey())
// 	// //打印查询结果
// 	// mpt.PrintQueryResult(kvpair1.GetKey(), qv1, qpf1)

// 	// //测试MPT query
// 	// qv2, qpf2 := mpt.QueryByKey(kvpair2.GetKey())
// 	// //打印查询结果
// 	// mpt.PrintQueryResult(kvpair2.GetKey(), qv2, qpf2)

// 	//测试MPT query
// 	// qv3, qpf3 := mpt.QueryByKey(kvpair3.GetKey())
// 	// //打印查询结果
// 	// mpt.PrintQueryResult(kvpair3.GetKey(), qv3, qpf3)

// 	//测试MPT query
// 	qverr, qpferr := mpt.QueryByKey("a77d344")
// 	//打印查询结果
// 	mpt.PrintQueryResult("a77d344", qverr, qpferr)

// 	// //测试MPT verify
// 	// mpt.VerifyQueryResult(qv1, qpf1)
// 	// mpt.VerifyQueryResult(qv2, qpf2)
// 	// mpt.VerifyQueryResult(qv3, qpf3)
// 	mpt.VerifyQueryResult(qverr, qpferr)

// 	// mpt.PrintMPT()
// }
