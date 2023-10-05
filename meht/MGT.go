package meht

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	// "MEHT/util"
)

//NewMGT() *MGT {}: NewMGT creates a empty MGT
//NewMGTNode(subNodes []*MGTNode, isLeaf bool, bucket *Bucket) *MGTNode {}: NewMGTNode creates a new MGTNode
//GetLeafNodeAndPath(bucketKey []int) []*MGTNode {}: 根据bucketKey,返回该bucket在MGT中的叶子节点,第0个是叶节点,最后一个是根节点
//GetOldBucketKey(bucket *Bucket) []int {}: GetOldBucketKey, 给定一个bucket,返回它的旧bucketKey
//MGTUpdate(newBuckets []*Bucket) *MGT {}: MGT生长,给定新的buckets,返回更新后的MGT
//MGTGrow(oldBucketKey []int, nodePath []*MGTNode, newBuckets []*Bucket) *MGT {}: MGT生长,给定旧bucketKey和新的buckets,返回更新后的MGT
//UpdateNodeHash(node *MGTNode) {}: 根据子节点哈希计算当前节点哈希
//PrintMGT() {}: 打印MGT
//GetProof(bucketKey []int) ([]byte, []MGTProof) {}: 给定bucketKey，返回它的mgtRootHash和mgtProof，不存在则返回nil
//ComputMGTRootHash(segRootHash []byte, mgtProof []MGTProof) []byte {}: 给定segRootHash和mgtProof，返回由它们计算得到的mgtRootHash
//PrintMGTProof(mgtProof []MGTProof) {}: 打印mgtProof

type MGTNode struct {
	nodeHash []byte // hash of this node, consisting of the hash of its children

	subNodes   []*MGTNode // sub-nodes in the tree, original given
	dataHashes [][]byte   // hashes of data elements, computed from subNodes

	isLeaf bool    // whether this node is a leaf node
	bucket *Bucket // bucket related to this leaf node
}

type MGT struct {
	Root *MGTNode // root node of the tree
}

// NewMGT creates a empty MGT
func NewMGT() *MGT {
	return &MGT{nil}
}

// NewMGTNode creates a new MGTNode
func NewMGTNode(subNodes []*MGTNode, isLeaf bool, bucket *Bucket) *MGTNode {
	var nodeHash []byte
	var dataHashes [][]byte

	//如果是叶子节点,遍历其所有segment,将每个segment的根hash加入dataHashes
	if isLeaf {
		for _, merkleTree := range bucket.GetMerkleTrees() {
			dataHashes = append(dataHashes, merkleTree.GetRootHash())
			nodeHash = append(nodeHash, merkleTree.GetRootHash()...)
		}
	} else {
		for i := 0; i < len(subNodes); i++ {
			dataHashes = append(dataHashes, subNodes[i].nodeHash)
			nodeHash = append(nodeHash, subNodes[i].nodeHash...)
		}
	}

	//对dataHashes求hash,得到nodeHash
	hash := sha256.Sum256(nodeHash)
	nodeHash = hash[:]

	return &MGTNode{nodeHash, subNodes, dataHashes, isLeaf, bucket}
}

// 根据bucketKey,返回该bucket在MGT中的叶子节点,第0个是叶节点,最后一个是根节点
func (mgt *MGT) GetLeafNodeAndPath(bucketKey []int) []*MGTNode {
	result := make([]*MGTNode, 0)
	//递归遍历根节点的所有子节点,找到bucketKey对应的叶子节点
	p := mgt.Root
	//将p插入到result的第0个位置
	result = append([]*MGTNode{p}, result...)
	//从根节点开始,逐层向下遍历,直到找到叶子节点
	for identI := len(bucketKey) - 1; identI >= 0; identI-- {
		if p == nil {
			return nil
		}
		p = p.subNodes[bucketKey[identI]]
		//将p插入到result的第0个位置
		result = append([]*MGTNode{p}, result...)
	}
	return result
}

// GetOldBucketKey, 给定一个bucket,返回它的旧bucketKey
func GetOldBucketKey(bucket *Bucket) []int {
	oldBucketKey := make([]int, 0)
	bucketKey := bucket.GetBucketKey()
	for i := 1; i < len(bucketKey); i++ {
		oldBucketKey = append(oldBucketKey, bucketKey[i])
	}
	return oldBucketKey
}

// MGT生长,给定新的buckets,返回更新后的MGT
func (mgt *MGT) MGTUpdate(newBuckets []*Bucket) *MGT {
	if len(newBuckets) == 0 {
		fmt.Printf("newBuckets is empty\n")
		return mgt
	}
	//如果root为空,则直接为newBuckets创建叶节点(newBuckets中只有一个bucket)
	if mgt.Root == nil {
		mgt.Root = NewMGTNode(nil, true, newBuckets[0])
		return mgt
	}

	var nodePath []*MGTNode
	//如果newBuckets中只有一个bucket，则说明没有发生分裂，只更新nodePath中所有的哈希值
	if len(newBuckets) == 1 {
		nodePath = mgt.GetLeafNodeAndPath(newBuckets[0].bucketKey)
		//更新叶子节点的dataHashes
		nodePath[0].dataHashes = nil
		for _, merkleTree := range newBuckets[0].GetMerkleTrees() {
			nodePath[0].dataHashes = append(nodePath[0].dataHashes, merkleTree.GetRootHash())
		}
		//更新叶子节点的nodeHash
		UpdateNodeHash(nodePath[0])
		//更新所有父节点的nodeHashs
		for i := 1; i < len(nodePath); i++ {
			nodePath[i].dataHashes[newBuckets[0].bucketKey[i-1]] = nodePath[i-1].nodeHash
			UpdateNodeHash(nodePath[i])
		}
	} else {
		//如果newBuckets中有多个bucket，则说明发生了分裂，MGT需要生长
		oldBucketKey := GetOldBucketKey(newBuckets[0])
		//根据旧bucketKey,找到旧bucket所在的叶子节点
		nodePath = mgt.GetLeafNodeAndPath(oldBucketKey)
		mgt.MGTGrow(oldBucketKey, nodePath, newBuckets)
	}
	return mgt
}

// MGT生长,给定旧bucketKey和新的buckets,返回更新后的MGT
func (mgt *MGT) MGTGrow(oldBucketKey []int, nodePath []*MGTNode, newBuckets []*Bucket) *MGT {
	//为每个新的bucket创建叶子节点,并插入到leafNode的subNodes中
	subNodes := make([]*MGTNode, 0)

	for _, bucket := range newBuckets {
		newNode := NewMGTNode(nil, true, bucket)
		subNodes = append(subNodes, newNode)
	}
	//创建新的父节点
	newFatherNode := NewMGTNode(subNodes, false, nil)

	//更新父节点的chid为新的父节点
	if len(nodePath) == 1 {
		mgt.Root = newFatherNode
		return mgt
	}
	nodePath[1].subNodes[oldBucketKey[0]] = newFatherNode
	UpdateNodeHash(nodePath[1])

	//更新所有父节点的nodeHash
	for i := 2; i < len(nodePath); i++ {
		nodePath[i].dataHashes[oldBucketKey[i-1]] = nodePath[i-1].nodeHash
		UpdateNodeHash(nodePath[i])
	}
	return mgt
}

// 根据子节点哈希计算当前节点哈希
func UpdateNodeHash(node *MGTNode) {
	var nodeHash []byte
	for _, dataHash := range node.dataHashes {
		nodeHash = append(nodeHash, dataHash...)
	}
	hash := sha256.Sum256(nodeHash)
	node.nodeHash = hash[:]
}

// 打印MGT
func (mgt *MGT) PrintMGT() {
	fmt.Printf("打印MGT-------------------------------------------------------------------------------------------\n")
	//递归打印MGT
	mgt.PrintMGTNode(mgt.Root, 0)
}

// 递归打印MGT
func (mgt *MGT) PrintMGTNode(node *MGTNode, level int) {
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
	for _, subNode := range node.subNodes {
		mgt.PrintMGTNode(subNode, level+1)
	}
}

type MGTProof struct {
	level    int    //哈希值所在的层数
	dataHash []byte //哈希值
}

// 给定bucketKey，返回它的mgtRootHash和mgtProof，不存在则返回nil
func (mgt *MGT) GetProof(bucketKey []int) ([]byte, []MGTProof) {
	//根据bucketKey,找到叶子节点和路径
	nodePath := mgt.GetLeafNodeAndPath(bucketKey)
	//找到mgtProof
	mgtProof := make([]MGTProof, 0)
	for i := 0; i < len(nodePath); i++ {
		for j := 0; j < len(nodePath[i].dataHashes); j++ {
			mgtProof = append(mgtProof, MGTProof{i, nodePath[i].dataHashes[j]})
		}
	}
	return mgt.Root.nodeHash, mgtProof
}

// 给定segRootHash和mgtProof，返回由它们计算得到的mgtRootHash
func ComputMGTRootHash(segRootHash []byte, mgtProof []MGTProof) []byte {
	//遍历mgtProof中前segNum个元素，如果segRootHash不存在，则返回nil，否则计算得到第0个node的nodeHash
	isSRHExist := false
	nodeHash0 := segRootHash
	var nodeHash1 []byte
	level := 0
	for i := 0; i < len(mgtProof); i++ {
		if mgtProof[i].level != level {
			if !isSRHExist {
				return nil
			} else {
				level++
				isSRHExist = false
				Hash := sha256.Sum256(nodeHash1)
				nodeHash0 = Hash[:]
				nodeHash1 = nodeHash1[:0]
				i--
			}
		} else {
			if bytes.Equal(nodeHash0, mgtProof[i].dataHash) {
				isSRHExist = true
			}
			nodeHash1 = append(nodeHash1, mgtProof[i].dataHash...)
		}
	}
	if !isSRHExist {
		return nil
	} else {
		Hash := sha256.Sum256(nodeHash1)
		nodeHash0 = Hash[:]
	}
	return nodeHash0
}

// 打印mgtProof
func PrintMGTProof(mgtProof []MGTProof) {
	for i := 0; i < len(mgtProof); i++ {
		fmt.Printf("[%d,%s]\n", mgtProof[i].level, hex.EncodeToString(mgtProof[i].dataHash))
	}
}

// func main() {
// 	//测试MGT
// 	mgt := meht.NewMGT() //branch

// 	kvpair1 := util.NewKVPair("0000", "value1")
// 	kvpair2 := util.NewKVPair("0001", "value2")
// 	kvpair3 := util.NewKVPair("1010", "value3")

// 	//创建bucket0
// 	bucket0 := meht.NewBucket(0, 2, 2, 1) //ld,rdx,capacity,segNum

// 	//插入kvpair1
// 	buckets := bucket0.Insert(*kvpair1)

// 	//更新MGT
// 	mgt = mgt.MGTUpdate(buckets)

// 	//打印MGT
// 	// mgt.PrintMGT()

// 	//插入kvpair2
// 	buckets = bucket0.Insert(*kvpair2)

// 	//更新MGT
// 	mgt = mgt.MGTUpdate(buckets)

// 	//打印MGT
// 	// mgt.PrintMGT()

// 	//插入kvpair3
// 	buckets = bucket0.Insert(*kvpair3)

// 	// //更新MGT
// 	mgt = mgt.MGTUpdate(buckets)

// 	// //打印MGT
// 	mgt.PrintMGT()

// }
