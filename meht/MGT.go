package meht

import (
	"MEHT/util"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"sort"
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

	isLeaf    bool    // whether this node is a leaf node
	bucket    *Bucket // bucket related to this leaf node
	bucketKey []int   // bucket key
}

type MGT struct {
	rdx         int      //radix of bucket key, decide the number of sub-nodes
	Root        *MGTNode // root node of the tree
	mgtRootHash []byte   // hash of this MGT, equals to the root node hash
	Latch       *sync.Mutex
}

// NewMGT creates a empty MGT
func NewMGT(rdx int) *MGT {
	return &MGT{rdx, nil, nil, &sync.Mutex{}}
}

// 获取root,如果root为空,则从leveldb中读取
func (mgt *MGT) GetRoot(db *leveldb.DB) *MGTNode {
	if mgt.Root == nil {
		mgtString, error_ := db.Get(mgt.mgtRootHash, nil)
		if error_ == nil {
			m, _ := DeserializeMGTNode(mgtString, mgt.rdx)
			mgt.Root = m
			mgt.Root.GetBucket(mgt.rdx, util.IntArrayToString(mgt.Root.bucketKey, mgt.rdx), db, nil)
		}
	}
	return mgt.Root
}

// 获取subnode,如果subnode为空,则从leveldb中读取
func (mgtNode *MGTNode) GetSubnode(index int, db *leveldb.DB, rdx int, cache *[]interface{}) *MGTNode {
	if mgtNode.subNodes[index] == nil {
		var node *MGTNode
		var ok bool
		if cache != nil {
			targetCache, _ := (*cache)[0].(*lru.Cache[string, *MGTNode])
			node, ok = targetCache.Get(string(mgtNode.dataHashes[index]))
		}
		if !ok {
			nodeString, error_ := db.Get(mgtNode.dataHashes[index], nil)
			if error_ != nil {
				panic(error_)
			}
			node, _ = DeserializeMGTNode(nodeString, rdx)
		}
		mgtNode.subNodes[index] = node
		if node.isLeaf {
			node.GetBucket(rdx, util.IntArrayToString(node.bucketKey, rdx), db, cache)
		}
	}
	return mgtNode.subNodes[index]
}

// 获取bucket,如果bucket为空,则从leveldb中读取
func (mgtNode *MGTNode) GetBucket(rdx int, name string, db *leveldb.DB, cache *[]interface{}) *Bucket {
	if mgtNode.bucket == nil {
		var ok bool
		var bucket *Bucket
		if cache != nil {
			targetCache, _ := (*cache)[1].(*lru.Cache[string, *Bucket])
			bucket, ok = targetCache.Get(name + "bucket" + util.IntArrayToString(mgtNode.bucketKey, rdx))
		}
		if !ok {
			bucketString, error_ := db.Get([]byte(name+"bucket"+util.IntArrayToString(mgtNode.bucketKey, rdx)), nil)
			if error_ == nil {
				bucket, _ = DeserializeBucket(bucketString)
			} else {
				panic(error_)
			}
		}
		mgtNode.bucket = bucket
	}
	return mgtNode.bucket
}

// 更新mgtRootHash,并将mgt存入leveldb
func (mgt *MGT) UpdateMGTToDB(db *leveldb.DB) []byte {
	//get the old mgtHash
	hash := sha256.Sum256(mgt.mgtRootHash)
	oldMgtHash := hash[:]
	//delete the old mgt in leveldb
	if err := db.Delete(oldMgtHash, nil); err != nil {
		panic(err)
	}
	// update mgtRootHash
	mgt.mgtRootHash = mgt.GetRoot(db).nodeHash
	//insert mgt in leveldb
	hash = sha256.Sum256(mgt.mgtRootHash)
	mgtHash := hash[:]
	if err := db.Put(mgtHash, SerializeMGT(mgt), nil); err != nil {
		panic(err)
	}
	return mgtHash
}

// NewMGTNode creates a new MGTNode
func NewMGTNode(subNodes []*MGTNode, isLeaf bool, bucket *Bucket, db *leveldb.DB, rdx int) *MGTNode {
	var nodeHash []byte
	var dataHashes [][]byte

	//如果是叶子节点,遍历其所有segment,将每个segment的根hash加入dataHashes
	if isLeaf {
		for _, merkleTree := range bucket.GetMerkleTrees() {
			dataHashes = append(dataHashes, merkleTree.GetRootHash())
			nodeHash = append(nodeHash, merkleTree.GetRootHash()...)
		}
	} else {
		if subNodes == nil {
			//subNodes是nil说明是非叶子节点,显然非叶子节点没有bucket,因此需要额外的rdx参数
			subNodes = make([]*MGTNode, rdx)
			dataHashes = make([][]byte, rdx)
		}
		for i := 0; i < len(subNodes); i++ {
			if subNodes[i] != nil {
				dataHashes = append(dataHashes, subNodes[i].nodeHash)
				nodeHash = append(nodeHash, subNodes[i].nodeHash...)
			}
		}
	}

	//对dataHashes求hash,得到nodeHash
	hash := sha256.Sum256(nodeHash)
	nodeHash = hash[:]
	var mgtNode *MGTNode
	//通过判断是否是叶子节点决定bucket是否需要
	if !isLeaf {
		mgtNode = &MGTNode{nodeHash, subNodes, dataHashes, isLeaf, nil, nil}
	} else {
		mgtNode = &MGTNode{nodeHash, subNodes, dataHashes, isLeaf, bucket, bucket.BucketKey}
	}
	//将mgtNode存入leveldb
	nodeString := SerializeMGTNode(mgtNode)
	if err := db.Put(nodeHash, nodeString, nil); err != nil {
		panic(err)
	}

	return mgtNode
}

// 更新nodeHash,并将node存入leveldb
func (mgtNode *MGTNode) UpdateMGTNodeToDB(db *leveldb.DB, cache *[]interface{}) {
	//delete the old node in leveldb
	if err := db.Delete(mgtNode.nodeHash, nil); err != nil {
		panic(err)
	}
	//update nodeHash
	UpdateNodeHash(mgtNode)
	//insert node in leveldb
	// fmt.Printf("When write MGTNode to DB, mgtNode.nodeHash: %x\n", mgtNode.nodeHash)
	if cache != nil {
		targetCache, _ := (*cache)[0].(*lru.Cache[string, *MGTNode])
		targetCache.Add(string(mgtNode.nodeHash), mgtNode)
	} else {
		if err := db.Put(mgtNode.nodeHash, SerializeMGTNode(mgtNode), nil); err != nil {
			panic(err)
		}
	}
}

// 根据bucketKey,返回该bucket在MGT中的叶子节点,第0个是叶节点,最后一个是根节点
func (mgt *MGT) GetLeafNodeAndPath(bucketKey []int, db *leveldb.DB, cache *[]interface{}) []*MGTNode {
	result := make([]*MGTNode, 0)
	//递归遍历根节点的所有子节点,找到bucketKey对应的叶子节点
	p := mgt.GetRoot(db)
	//将p插入到result的第0个位置
	result = append(result, p)
	//从根节点开始,逐层向下遍历,直到找到叶子节点
	if len(bucketKey) == 0 {
		return result
	}
	for identI := len(bucketKey) - 1; identI >= 0; identI-- {
		if p == nil {
			return nil
		}
		p = p.GetSubnode(bucketKey[identI], db, mgt.rdx, cache)
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
	//这里能用切片吗？
	//if len(bucketKey) > 0 {
	//	return bucketKey[1:]
	//} else {
	//	return nil
	//}
	return oldBucketKey
}

// MGT生长,给定新的buckets,返回更新后的MGT
func (mgt *MGT) MGTUpdate(newBucketss [][]*Bucket, db *leveldb.DB, cache *[]interface{}) *MGT {
	if len(newBucketss) == 0 {
		//fmt.Printf("newBuckets is empty\n")
		return mgt
	}
	//如果root为空,则直接为newBuckets创建叶节点(newBuckets中只有一个bucket)
	if mgt.GetRoot(db) == nil {
		mgt.Root = NewMGTNode(nil, true, newBucketss[0][0], db, newBucketss[0][0].rdx)
		mgt.Root.UpdateMGTNodeToDB(db, cache)
		return mgt
	}

	var nodePath []*MGTNode
	//如果newBuckets中只有一个bucket，则说明没有发生分裂，只更新nodePath中所有的哈希值
	//连环分裂至少需要第一层有rdx个桶，因此只需要判断第一层是不是一个桶就知道bucketss里是不是只有一个桶
	if len(newBucketss[0]) == 1 {
		bk := newBucketss[0][0]
		nodePath = mgt.GetLeafNodeAndPath(bk.BucketKey, db, cache)
		targetMerkleTrees := bk.GetMerkleTrees()
		//更新叶子节点的dataHashes
		nodePath[0].dataHashes = make([][]byte, 0)
		segKeyInorder := make([]string, 0)
		for segKey := range bk.GetMerkleTrees() {
			segKeyInorder = append(segKeyInorder, segKey)
		}
		sort.Strings(segKeyInorder)
		for _, key := range segKeyInorder {
			//if targetMerkleTrees[key] == nil {
			//	bk.GetMerkleTree(key, db)
			//}
			nodePath[0].dataHashes = append(nodePath[0].dataHashes, targetMerkleTrees[key].GetRootHash())
		}
		//更新叶子节点的nodeHash,并将叶子节点存入leveldb
		nodePath[0].UpdateMGTNodeToDB(db, cache)
		//更新所有父节点的nodeHashs,并将父节点存入leveldb
		for i := 1; i < len(nodePath); i++ {
			nodePath[i].dataHashes[bk.BucketKey[i-1]] = nodePath[i-1].nodeHash
			nodePath[i].UpdateMGTNodeToDB(db, cache)
		}
	} else {
		//如果newBuckets中有多个bucket，则说明发生了分裂，MGT需要生长
		//分层依次生长,这样每次都只需要在一条路径上多扩展出一层mgtLeafNodes
		for _, newBuckets := range newBucketss {
			var oldBucketKey []int
			// oldBucketKey应该由非连环分裂的桶决定，因为连环分裂桶的ld会因为下一层的更新而加一，连环分裂桶的bk也会因此比同一层的其他桶长一些
			if len(newBuckets[0].BucketKey) <= len(newBuckets[1].BucketKey) {
				oldBucketKey = GetOldBucketKey(newBuckets[0])
			} else {
				oldBucketKey = GetOldBucketKey(newBuckets[1])
			}
			//fmt.Printf("oldBucketKey: %s\n", util.IntArrayToString(oldBucketKey, newBuckets[0].rdx))
			//根据旧bucketKey,找到旧bucket所在的叶子节点
			nodePath = mgt.GetLeafNodeAndPath(oldBucketKey, db, cache)
			mgt.MGTGrow(oldBucketKey, nodePath, newBuckets, db, cache)
		}
		//oldBucketKey := GetOldBucketKey(newBuckets[0])
		////fmt.Printf("oldBucketKey: %s\n", util.IntArrayToString(oldBucketKey, newBuckets[0].rdx))
		////根据旧bucketKey,找到旧bucket所在的叶子节点
		//nodePath = mgt.GetLeafNodeAndPath(oldBucketKey, db, newBuckets[0].rdx)
		//mgt.MGTGrow(oldBucketKey, nodePath, newBuckets, db)
	}
	return mgt
}

// MGT生长,给定旧bucketKey和新的buckets,返回更新后的MGT
func (mgt *MGT) MGTGrow(oldBucketKey []int, nodePath []*MGTNode, newBuckets []*Bucket, db *leveldb.DB, cache *[]interface{}) *MGT {
	//为每个新的bucket创建叶子节点,并插入到leafNode的subNodes中
	subNodes := make([]*MGTNode, 0)

	for i := 0; i < len(newBuckets); i++ {
		newNode := NewMGTNode(nil, true, newBuckets[i], db, newBuckets[0].rdx)
		subNodes = append(subNodes, newNode)
		newNode.UpdateMGTNodeToDB(db, cache)
	}
	//创建新的父节点
	newFatherNode := NewMGTNode(subNodes, false, nil, db, newBuckets[0].rdx)
	newFatherNode.UpdateMGTNodeToDB(db, cache)

	//更新父节点的chid为新的父节点
	if len(nodePath) == 1 {
		mgt.Root = newFatherNode
		mgt.Root.UpdateMGTNodeToDB(db, cache)
		return mgt
	}
	nodePath[1].subNodes[oldBucketKey[0]] = newFatherNode
	nodePath[1].UpdateMGTNodeToDB(db, cache)

	//更新所有父节点的nodeHash
	for i := 2; i < len(nodePath); i++ {
		nodePath[i].dataHashes[oldBucketKey[i-1]] = nodePath[i-1].nodeHash
		nodePath[i].UpdateMGTNodeToDB(db, cache)
	}
	return mgt
}

// 根据子节点哈希计算当前节点哈希
func UpdateNodeHash(node *MGTNode) {
	var nodeHash []byte
	var keys []int
	for key := range node.dataHashes {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i int, j int) bool {
		return i < j
	})
	for _, key := range keys {
		nodeHash = append(nodeHash, node.dataHashes[key]...)
	}
	hash := sha256.Sum256(nodeHash)
	node.nodeHash = hash[:]
}

// 打印MGT
func (mgt *MGT) PrintMGT(mehtName string, db *leveldb.DB, cache *[]interface{}) {
	fmt.Printf("打印MGT-------------------------------------------------------------------------------------------\n")
	if mgt == nil {
		return
	}
	//递归打印MGT
	fmt.Printf("MGTRootHash: %x\n", mgt.mgtRootHash)
	mgt.PrintMGTNode(mehtName, mgt.GetRoot(db), 0, db, cache)

}

// 递归打印MGT
func (mgt *MGT) PrintMGTNode(mehtName string, node *MGTNode, level int, db *leveldb.DB, cache *[]interface{}) {
	if node == nil {
		return
	}

	fmt.Printf("Level: %d--------------------------------------------------------------------------\n", level)

	if node.isLeaf {
		fmt.Printf("Leaf Node: %s\n", hex.EncodeToString(node.nodeHash))
		fmt.Printf("bucketKey: %s\n", util.IntArrayToString(node.bucketKey, mgt.rdx))
	} else {
		fmt.Printf("Internal Node: %s\n", hex.EncodeToString(node.nodeHash))
	}
	fmt.Printf("dataHashes:\n")
	for _, dataHash := range node.dataHashes {
		fmt.Printf("%s\n", hex.EncodeToString(dataHash))
	}
	for i := 0; i < len(node.dataHashes); i++ {
		if !node.isLeaf && node.dataHashes[i] != nil {
			mgt.PrintMGTNode(mehtName, node.GetSubnode(i, db, mgt.rdx, cache), level+1, db, cache)
		}
	}
}

type MGTProof struct {
	level    int    //哈希值所在的层数
	dataHash []byte //哈希值
}

// 给定bucketKey，返回它的mgtRootHash和mgtProof，不存在则返回nil
func (mgt *MGT) GetProof(bucketKey []int, db *leveldb.DB, cache *[]interface{}) ([]byte, []MGTProof) {
	//根据bucketKey,找到叶子节点和路径
	nodePath := mgt.GetLeafNodeAndPath(bucketKey, db, cache)
	//找到mgtProof
	mgtProof := make([]MGTProof, 0)
	for i := 0; i < len(nodePath); i++ {
		if nodePath[i] == nil {
			mgtProof = append(mgtProof, MGTProof{i, nil})
			break
		}
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
	level := mgtProof[len(mgtProof)-1].level
	for i := len(mgtProof) - 1; i >= 0; i-- {
		if mgtProof[i].level != level {
			if !isSRHExist {
				return nil
			} else {
				level--
				isSRHExist = false
				Hash := sha256.Sum256(nodeHash1)
				nodeHash0 = Hash[:]
				nodeHash1 = nodeHash1[:0]
				i++
			}
		} else {
			if bytes.Equal(nodeHash0, mgtProof[i].dataHash) {
				isSRHExist = true
			}
			nodeHash1 = append(mgtProof[i].dataHash, nodeHash1...)
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

type SeMGT struct {
	Rdx         int    //radix of bucket key, decide the number of sub-nodes
	MgtRootHash []byte // hash of this MGT, equals to the hash of the root node hash
}

func SerializeMGT(mgt *MGT) []byte {
	seMGT := &SeMGT{mgt.rdx, mgt.mgtRootHash}
	jsonMGT, err := json.Marshal(seMGT)
	if err != nil {
		fmt.Printf("SerializeMGT error: %v\n", err)
		return nil
	}
	return jsonMGT
}

func DeserializeMGT(data []byte) (*MGT, error) {
	var seMGT SeMGT
	err := json.Unmarshal(data, &seMGT)
	if err != nil {
		fmt.Printf("DeserializeMGT error: %v\n", err)
		return nil, err
	}
	mgt := &MGT{seMGT.Rdx, nil, seMGT.MgtRootHash, &sync.Mutex{}}
	return mgt, nil
}

type SeMGTNode struct {
	NodeHash   []byte // hash of this node, consisting of the hash of its children
	DataHashes string // hashes of data elements, computed from subNodes, is used for indexing children nodes in leveldb

	IsLeaf    bool  // whether this node is a leaf node
	BucketKey []int // bucketkey related to this leaf node,is used for indexing bucket in leveldb
}

func SerializeMGTNode(node *MGTNode) []byte {
	dataHashString := ""
	for i := 0; i < len(node.dataHashes); i++ {
		dataHashString += hex.EncodeToString(node.dataHashes[i])
		if i != len(node.dataHashes)-1 {
			dataHashString += ","
		}
	}
	// fmt.Printf("dataHashString is %s\n", dataHashString)
	seMGTNode := &SeMGTNode{node.nodeHash, dataHashString, node.isLeaf, node.bucketKey}
	jsonMGTNode, err := json.Marshal(seMGTNode)
	if err != nil {
		fmt.Printf("SerializeMGTNode error: %v\n", err)
		return nil
	}
	return jsonMGTNode
}

func DeserializeMGTNode(data []byte, rdx int) (*MGTNode, error) {
	var seMGTNode SeMGTNode
	err := json.Unmarshal(data, &seMGTNode)
	if err != nil {
		fmt.Printf("DeserializeMGTNode error: %v\n", err)
		return nil, err
	}
	dataHashes := make([][]byte, 0)
	dataHashStrings := strings.Split(seMGTNode.DataHashes, ",")
	for i := 0; i < len(dataHashStrings); i++ {
		dataHash, _ := hex.DecodeString(dataHashStrings[i])
		dataHashes = append(dataHashes, dataHash)
	}
	//subnodes := make([]*MGTNode, len(dataHashes))
	subnodes := make([]*MGTNode, rdx)
	mgtNode := &MGTNode{seMGTNode.NodeHash, subnodes, dataHashes, seMGTNode.IsLeaf, nil, seMGTNode.BucketKey}
	return mgtNode, nil
}

// func main() {
// //测试MGT
// mgt := meht.NewMGT() //branch

// kvpair1 := util.NewKVPair("0000", "value1")
// kvpair2 := util.NewKVPair("0001", "value2")
// kvpair3 := util.NewKVPair("1010", "value3")

// //创建bucket0
// bucket0 := meht.NewBucket(mehtName, 0, 2, 2, 1) //ld,rdx,capacity,segNum

// //插入kvpair1
// buckets := bucket0.Insert(kvpair1, db)
// bucket0.PrintBucket(db)

// //更新MGT
// mgt = mgt.MGTUpdate(buckets, db)

// //打印MGT
// mgt.PrintMGT(db)

// //插入kvpair2
// buckets = bucket0.Insert(kvpair2, db)
// bucket0.PrintBucket(db)

// //更新MGT
// mgt = mgt.MGTUpdate(buckets, db)

// //打印MGT
// mgt.PrintMGT(db)

// //插入kvpair3
// buckets = bucket0.Insert(kvpair3, db)
// for i := 0; i < len(buckets); i++ {
// 	buckets[i].PrintBucket(db)
// }

// // //更新MGT
// mgt = mgt.MGTUpdate(buckets, db)

// // //打印MGT
// mgt.PrintMGT(db)

// mgt.UpdateMGTToDB(db)

// }
