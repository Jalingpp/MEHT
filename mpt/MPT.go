package mpt

import (
	"MEHT/util"
	"bytes"
	"crypto/sha256"
	"fmt"
)

//NewShortNode(prefix []byte, isLeaf bool, suffix []byte, nextNode *FullNode, value []byte) *ShortNode {}: creates a ShortNode and computes its nodeHash
//UpdateShortNodeHash(sn *ShortNode) {}: 更新ShortNode的nodeHash
//NewFullNode(children [16]*ShortNode) *FullNode {}: creates a new FullNode and computes its nodeHash
//UpdateFullNodeHash(fn *FullNode) {}: updates the nodeHash of a FullNode
//Insert(kvpair util.KVPair) []byte {}: 插入一个KVPair到MPT中,返回新的根节点的哈希值
//PrintMPT() {}: 打印MPT
//QueryByKey(key string) (string, *MPTProof) {}: 根据key查询value，返回value和证明
//PrintQueryResult(key string, value string, mptProof *MPTProof) {}: 打印查询结果
//VerifyQueryResult(value string, mptProof *MPTProof) bool {}: 验证查询结果

type FullNode struct {
	nodeHash     []byte         //当前节点的哈希值,由childrenHash计算得到
	children     [16]*ShortNode // children的指针，0-15表示0-9,a-f
	childrenHash [16][]byte     // children的哈希值
	value        []byte         // 以前面ExtensionNode的prefix+suffix为key的value
}

type ShortNode struct {
	nodeHash []byte //当前节点的哈希值，由prefix+suffix+value/nextNodeHash计算得到

	prefix []byte //前缀

	isLeaf       bool      //是否是叶子节点
	suffix       []byte    //后缀，shared nibble（extension node）或key-end（leaf node）
	nextNode     *FullNode //下一个FullNode节点(当前节点是extension node时)
	nextNodeHash []byte    //下一个FullNode节点的哈希值
	value        []byte    //value（当前节点是leaf node时）
}

type MPT struct {
	root *ShortNode //根节点
}

// NewMPT creates a empty MPT
func NewMPT() *MPT {
	return &MPT{nil}
}

// NewShortNode creates a ShortNode and computes its nodeHash
func NewShortNode(prefix []byte, isLeaf bool, suffix []byte, nextNode *FullNode, value []byte) *ShortNode {
	nodeHash := append(prefix, suffix...)
	var nextNodeHash []byte
	if isLeaf {
		nodeHash = append(nodeHash, value...)
		nextNodeHash = nil
	} else {
		nodeHash = append(nodeHash, nextNode.nodeHash...)
		nextNodeHash = nextNode.nodeHash
	}
	hash := sha256.Sum256(nodeHash)
	nodeHash = hash[:]
	return &ShortNode{nodeHash, prefix, isLeaf, suffix, nextNode, nextNodeHash, value}
}

// UpdateShortNodeHash 更新ShortNode的nodeHash
func UpdateShortNodeHash(sn *ShortNode) {
	nodeHash := append(sn.prefix, sn.suffix...)
	if sn.isLeaf {
		nodeHash = append(nodeHash, sn.value...)
	} else {
		nodeHash = append(nodeHash, sn.nextNode.nodeHash...)
	}
	hash := sha256.Sum256(nodeHash)
	sn.nodeHash = hash[:]
}

// NewFullNode creates a new FullNode and computes its nodeHash
func NewFullNode(children [16]*ShortNode) *FullNode {
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
	hash := sha256.Sum256(nodeHash)
	nodeHash = hash[:]
	return &FullNode{nodeHash, children, childrenHash, nil}
}

// UpdateFullNodeHash updates the nodeHash of a FullNode
func UpdateFullNodeHash(fn *FullNode) {
	var nodeHash []byte
	for i := 0; i < 16; i++ {
		nodeHash = append(nodeHash, fn.childrenHash[i]...)
	}
	nodeHash = append(nodeHash, fn.value...)
	hash := sha256.Sum256(nodeHash)
	fn.nodeHash = hash[:]
}

// 插入一个KVPair到MPT中，返回新的根节点的哈希值
func (mpt *MPT) Insert(kvpair *util.KVPair) []byte {
	//判断是否为第一次插入
	if mpt.root == nil {
		//创建一个ShortNode
		mpt.root = NewShortNode(nil, true, []byte(kvpair.GetKey()), nil, []byte(kvpair.GetValue()))
		return mpt.root.nodeHash
	}
	//如果不是第一次插入，递归插入
	mpt.root = mpt.RecursiveInsertShortNode(nil, []byte(kvpair.GetKey()), []byte(kvpair.GetValue()), mpt.root)
	return mpt.root.nodeHash
}

// 递归插入当前MPT Node
func (mpt *MPT) RecursiveInsertShortNode(prefix []byte, suffix []byte, value []byte, cnode *ShortNode) *ShortNode {
	//如果当前节点是叶子节点
	if cnode.isLeaf {
		//判断当前suffix是否和suffix相同，如果相同，更新value，否则新建一个ExtensionNode，一个BranchNode，一个LeafNode，将两个LeafNode插入到FullNode中
		if bytes.Equal(cnode.suffix, suffix) {
			cnode.value = value
			UpdateShortNodeHash(cnode)
			return cnode
		} else {
			//获取两个suffix的共同前缀
			comprefix := util.CommPrefix(cnode.suffix, suffix)
			//更新当前叶节点的prefix和suffix，nodeHash
			cnode.prefix = append(cnode.prefix, cnode.suffix[0:len(comprefix)+1]...)
			cnode.suffix = cnode.suffix[len(comprefix)+1:]
			UpdateShortNodeHash(cnode)
			//新建一个LeafNode
			newLeaf := NewShortNode(append(prefix, suffix[0:len(comprefix)+1]...), true, suffix[len(comprefix)+1:], nil, value)
			//创建一个BranchNode
			var children [16]*ShortNode
			children[util.ByteToHexIndex(cnode.prefix[len(cnode.prefix)-1])] = cnode
			children[util.ByteToHexIndex(suffix[len(comprefix)])] = newLeaf
			newBranch := NewFullNode(children)
			//创建一个ExtensionNode，其prefix为之前的prefix，其suffix为comprefix，其nextNode为一个FullNode
			newExtension := NewShortNode(prefix, false, comprefix, newBranch, nil)
			return newExtension
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
			fullnode := mpt.RecursiveInsertFullNode(append(prefix, commPrefix...), suffix[len(commPrefix):], value, cnode.nextNode)
			cnode.nextNode = fullnode
			cnode.nextNodeHash = fullnode.nodeHash
			UpdateShortNodeHash(cnode)
			return cnode
		} else {
			//如果当前节点的suffix和suffix有部分公共前缀
			//更新当前节点的prefix和suffix，nodeHash
			cnode.prefix = append(cnode.prefix, suffix[0:len(commPrefix)+1]...)
			cnode.suffix = nil
			UpdateShortNodeHash(cnode)
			//新建一个LeafNode
			newLeaf := NewShortNode(append(prefix, suffix[0:len(commPrefix)+1]...), true, suffix[len(commPrefix)+1:], nil, value)
			//创建一个BranchNode
			var children [16]*ShortNode
			children[util.ByteToHexIndex(cnode.prefix[len(cnode.prefix)-1])] = cnode
			children[util.ByteToHexIndex(suffix[len(commPrefix)])] = newLeaf
			newBranch := NewFullNode(children)
			//创建一个ExtensionNode，其prefix为之前的prefix，其suffix为comprefix，其nextNode为一个FullNode
			newExtension := NewShortNode(prefix, false, commPrefix, newBranch, nil)
			return newExtension
		}
	}
}

func (mpt *MPT) RecursiveInsertFullNode(prefix []byte, suffix []byte, value []byte, cnode *FullNode) *FullNode {
	//如果当前节点是FullNode
	//如果len(suffix)==0，则value插入到当前FullNode的value中；否则，递归插入到children中
	if len(suffix) == 0 {
		cnode.value = value
		UpdateFullNodeHash(cnode)
		return cnode
	} else {
		var childnode *ShortNode
		if cnode.children[util.ByteToHexIndex(suffix[0])] != nil {
			childnode = mpt.RecursiveInsertShortNode(append(prefix, suffix[0]), suffix[1:], value, cnode.children[util.ByteToHexIndex(suffix[0])])
		} else {
			childnode = NewShortNode(append(prefix, suffix[0]), true, suffix[1:], nil, value)
		}
		cnode.children[util.ByteToHexIndex(suffix[0])] = childnode
		cnode.childrenHash[util.ByteToHexIndex(suffix[0])] = childnode.nodeHash
		UpdateFullNodeHash(cnode)
		return cnode
	}
}

// 打印MPT
func (mpt *MPT) PrintMPT() {
	if mpt.root == nil {
		return
	}
	mpt.RecursivePrintShortNode(mpt.root, 0)
}

// 递归打印ShortNode
func (mpt *MPT) RecursivePrintShortNode(cnode *ShortNode, level int) {
	//如果当前节点是叶子节点
	if cnode.isLeaf {
		//打印当前叶子节点
		fmt.Printf("level: %d, leafNode:%x\n", level, cnode.nodeHash)
		fmt.Printf("prefix:%x, suffix:%x, value:%x\n", cnode.prefix, cnode.suffix, cnode.value)
	} else {
		//打印当前Extension节点
		fmt.Printf("level: %d, extensionNode:%x\n", level, cnode.nodeHash)
		fmt.Printf("prefix:%x, suffix:%x, next node:%x\n", cnode.prefix, cnode.suffix, cnode.nextNodeHash)
		//递归打印nextNode
		mpt.RecursivePrintFullNode(cnode.nextNode, level+1)
	}
}

// 递归打印FullNode
func (mpt *MPT) RecursivePrintFullNode(cnode *FullNode, level int) {
	//打印当前FullNode
	fmt.Printf("level: %d, fullNode:%x, value:%x\n", level, cnode.nodeHash, cnode.value)
	//打印所有孩子节点的hash
	for i := 0; i < 16; i++ {
		if cnode.children[i] != nil {
			fmt.Printf("children[%d]:%x\n", i, cnode.children[i].nodeHash)
		}
	}
	//递归打印所有孩子节点
	for i := 0; i < 16; i++ {
		if cnode.children[i] != nil {
			mpt.RecursivePrintShortNode(cnode.children[i], level+1)
		}
	}
}

// 根据key查询value，返回value和证明
func (mpt *MPT) QueryByKey(key string) (string, *MPTProof) {
	//如果MPT为空，返回空
	if mpt.root == nil {
		return "", &MPTProof{false, 0, nil}
	}
	//递归查询
	return mpt.RecursiveQueryShortNode([]byte(key), 0, 0, mpt.root)
}

func (mpt *MPT) RecursiveQueryShortNode(key []byte, p int, level int, cnode *ShortNode) (string, *MPTProof) {
	if cnode == nil {
		return "", &MPTProof{false, 0, nil}
	}
	//当前节点是叶子节点
	if cnode.isLeaf {
		//构造当前节点的证明
		proofElement := NewProofElement(level, 0, cnode.prefix, cnode.suffix, cnode.value, nil, [16][]byte{})
		//找到对应key的value
		if bytes.Equal(cnode.suffix, key[p:]) {
			return string(cnode.value), &MPTProof{true, level, []*ProofElement{proofElement}}
		} else {
			return "", &MPTProof{false, level, []*ProofElement{proofElement}}
		}
	} else {
		//当前节点是ExtensionNode
		//构造当前节点的证明
		proofElement := NewProofElement(level, 1, cnode.prefix, cnode.suffix, nil, cnode.nextNodeHash, [16][]byte{})
		//当前节点的suffix被key的suffix完全包含，则继续递归查询nextNode，将子查询结果与当前结果合并返回
		if len(util.CommPrefix(cnode.suffix, key[p:])) == len(cnode.suffix) {
			valuestr, mptProof := mpt.RecursiveQueryFullNode(key, p+len(cnode.suffix), level+1, cnode.nextNode)
			proofElements := append(mptProof.GetProofs(), proofElement)
			return valuestr, &MPTProof{mptProof.GetIsExist(), mptProof.GetLevels(), proofElements}
		} else {
			return "", &MPTProof{false, level, []*ProofElement{proofElement}}
		}
	}
}

func (mpt *MPT) RecursiveQueryFullNode(key []byte, p int, level int, cnode *FullNode) (string, *MPTProof) {
	proofElement := NewProofElement(level, 2, nil, nil, cnode.value, nil, cnode.childrenHash)
	if p >= len(key) {
		//判断当前FullNode是否有value，如果有，构造存在证明返回
		if cnode.value != nil {
			return string(cnode.value), &MPTProof{true, level, []*ProofElement{proofElement}}
		} else {
			return "", &MPTProof{false, level, []*ProofElement{proofElement}}
		}
	}
	//如果当前FullNode的children中没有对应的key[p]，则构造不存在证明返回
	if cnode.children[util.ByteToHexIndex(key[p])] == nil {
		return "", &MPTProof{false, level, []*ProofElement{proofElement}}
	}
	//如果当前FullNode的children中有对应的key[p]，则递归查询children，将子查询结果与当前结果合并返回
	valuestr, mptProof := mpt.RecursiveQueryShortNode(key, p+1, level+1, cnode.children[util.ByteToHexIndex(key[p])])
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
			nodeHash1 = append(proof.prefix, proof.suffix...)
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
			nodeHash1 = append(proof.prefix, proof.suffix...)
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
