package mht

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

//NewMerkleNode(left, right *MerkleNode, data []byte) *MerkleNode {}: 创建一个新的默克尔树节点
//NewEmptyMerkleTree() *MerkleTree {}: 新建一个空的默克尔树
//NewMerkleTree(data [][]byte) *MerkleTree {}: 构建一个新的默克尔树
//GetRoot() *MerkleNode {} :获取默克尔树的根节点
//GetRootHash() []byte {}:获取默克尔树的根节点的哈希值
//UpdateRoot(i int, data []byte) []byte {}:修改data中第i个数据后更新默克尔树的根节点,返回新的根节点哈希
//PrintTree() {}:打印整个默克尔树
//GetProof(i int) MHTProof {}:返回某个叶子节点的默克尔证明
//InsertData(data []byte) []byte {}:插入一个data,更新默克尔树,返回新的根节点哈希

// MerkleNode 表示默克尔树的节点
type MerkleNode struct {
	Left   *MerkleNode
	Right  *MerkleNode
	Parent *MerkleNode
	Data   []byte
}

// MerkleTree 表示默克尔树
type MerkleTree struct {
	Root      *MerkleNode
	DataList  [][]byte
	LeafNodes []*MerkleNode
}

// NewMerkleNode 创建一个新的默克尔树节点
func NewMerkleNode(left, right *MerkleNode, data []byte) *MerkleNode {
	node := MerkleNode{}
	if left == nil && right == nil {
		hash := sha256.Sum256(data)
		node.Data = hash[:]
	} else {
		prevHashes := append(left.Data, right.Data...)
		hash := sha256.Sum256(prevHashes)
		node.Data = hash[:]
	}
	node.Left = left
	node.Right = right
	node.Parent = nil
	return &node
}

// 新建一个空的默克尔树
func NewEmptyMerkleTree() *MerkleTree {
	return &MerkleTree{Root: nil, DataList: make([][]byte, 0), LeafNodes: nil}
}

// NewMerkleTree 构建一个新的默克尔树
func NewMerkleTree(data [][]byte) *MerkleTree {
	//用data创建一个dataList,并复制data的值到dataList中
	dataList := make([][]byte, len(data))
	for i := 0; i < len(data); i++ {
		copiedData := make([]byte, len(data[i]))
		copy(copiedData, data[i])
		dataList[i] = copiedData
	}
	var nodes []*MerkleNode
	var leafnodes []*MerkleNode

	// 创建叶子节点
	for i := 0; i < len(data); i++ {
		node := NewMerkleNode(nil, nil, data[i])
		nodes = append(nodes, node)
		leafnodes = append(leafnodes, node)
	}

	// 构建树
	for len(nodes) > 1 {
		newLevel := make([]*MerkleNode, 0)
		for i := 0; i < len(nodes); i += 2 {
			if i+1 < len(nodes) {
				node := NewMerkleNode(nodes[i], nodes[i+1], nil) //data字段在新建节点时根据左右子节点的data字段计算得到
				nodes[i].Parent = node
				nodes[i+1].Parent = node
				newLevel = append(newLevel, node)
			} else {
				newLevel = append(newLevel, nodes[i])
			}
		}
		nodes = newLevel
	}

	root := nodes[0]
	return &MerkleTree{Root: root, DataList: dataList, LeafNodes: leafnodes}
}

// 获取默克尔树的根节点
func (tree *MerkleTree) GetRoot() *MerkleNode {
	return tree.Root
}

// 获取默克尔树的根节点的哈希值
func (tree *MerkleTree) GetRootHash() []byte {
	return tree.Root.Data
}

// 修改data中第i个数据后更新默克尔树的根节点,返回新的根节点哈希
func (tree *MerkleTree) UpdateRoot(i int, data []byte) []byte {
	// fmt.Printf("value byte:%x\n", data)
	tree.DataList[i] = data
	//修改叶子节点
	hash := sha256.Sum256(data)
	tree.LeafNodes[i].Data = hash[:]
	//fmt.Printf("data:%x\n", hash[:])
	//递归修改父节点
	updataParentData(tree.LeafNodes[i].Parent)
	return tree.Root.Data
}

// 递归修改父节点的data
func updataParentData(node *MerkleNode) {
	if node == nil {
		return
	}
	prevHashes := make([]byte, 0)
	if node.Left != nil {
		//fmt.Printf("Left Data: %x\n", node.Left.Data)
		prevHashes = append(prevHashes, node.Left.Data...)
	}
	if node.Right != nil {
		//fmt.Printf("Right Data: %x\n", node.Right.Data)
		prevHashes = append(prevHashes, node.Right.Data...)
	}
	hash := sha256.Sum256(prevHashes)
	node.Data = hash[:]
	updataParentData(node.Parent)
}

// 打印整个默克尔树
func (tree *MerkleTree) PrintTree() {
	tree.Root.PrintNode()
}

// 打印一个节点
func (node *MerkleNode) PrintNode() {
	if node == nil {
		return
	}
	node.Left.PrintNode()
	node.Right.PrintNode()
	fmt.Printf("%s\n", hex.EncodeToString(node.Data))
}

// 返回某个叶子节点的默克尔证明
func (tree *MerkleTree) GetProof(i int) *MHTProof {
	var proof []ProofPair
	node := tree.LeafNodes[i]
	for node.Parent != nil {
		if node.Parent.Left == node {
			proof = append(proof, ProofPair{1, node.Parent.Right.Data})
		} else {
			proof = append(proof, ProofPair{0, node.Parent.Left.Data})
		}
		node = node.Parent
	}
	return &MHTProof{true, proof, false, nil, nil, nil}
}

// 插入一个data,更新默克尔树,返回新的根节点哈希
func (tree *MerkleTree) InsertData(data []byte) []byte {
	if tree.DataList == nil {
		tree.DataList = make([][]byte, 0)
	}
	tree.DataList = append(tree.DataList, data)
	//重建默克尔树
	newTree := NewMerkleTree(tree.DataList)
	tree.Root = newTree.Root
	tree.LeafNodes = newTree.LeafNodes
	return tree.Root.Data
}

type SeMHT struct {
	DataList string // data list of the merkle tree, is used for reconstructing the merkle tree
}

// 序列化默克尔树
func SerializeMHT(mht *MerkleTree) []byte {
	dataListstring := ""
	for i := 0; i < len(mht.DataList); i++ {
		data := hex.EncodeToString(mht.DataList[i])
		dataListstring += data
		// fmt.Printf("data[%d]=%x\n", i, data)
		if i < len(mht.DataList)-1 {
			dataListstring += ","
		}
	}
	seMHT := &SeMHT{dataListstring}
	if jsonMHT, err := json.Marshal(seMHT); err != nil {
		fmt.Printf("SerializeMHT error: %v\n", err)
		return nil
	} else {
		return jsonMHT
	}
}

// 反序列化默克尔树
func DeserializeMHT(data []byte) (*MerkleTree, error) {
	var seMHT SeMHT
	if err := json.Unmarshal(data, &seMHT); err != nil {
		fmt.Printf("DeserializeMHT error: %v\n", err)
		return nil, err
	}
	dataList := make([][]byte, 0)
	dataListstrings := strings.Split(seMHT.DataList, ",")
	for i := 0; i < len(dataListstrings); i++ {
		data, _ := hex.DecodeString(dataListstrings[i])
		// fmt.Printf("data[%d]=%x\n", i, data)
		dataList = append(dataList, data)
	}
	mht := NewMerkleTree(dataList)
	return mht, nil
}

// func main() {
// 	data := [][]byte{
// 		[]byte("data1"),
// 		[]byte("data2"),
// 		[]byte("data3"),
// 		[]byte("data4"),
// 	}

// 	// 创建默克尔树
// 	tree := NewMerkleTree(data)

// 	// 打印根节点的哈希值
// 	fmt.Printf("Merkle Root Hash: %s\n", hex.EncodeToString(tree.Root.Data))
// }
