package meht

import (
	"MEHT/util"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
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

	cachedNodes      []*MGTNode // cached nodes
	cachedDataHashes [][]byte   //datahashes of cached nodes

	//NOTE：如果cached node是中间节点（isLeaf是false），则表明发生了分裂

	isLeaf    bool    // whether this node is a leaf node
	bucket    *Bucket // bucket related to this leaf node
	bucketKey []int   // bucket key
}

type MGT struct {
	rdx         int      //radix of bucket key, decide the number of sub-nodes
	Root        *MGTNode // root node of the tree
	mgtRootHash []byte   // hash of this MGT, equals to the root node hash

	cachedLNMap map[string]bool //当前被缓存在中间节点的叶子节点，存在bool为true，不存在就直接没有，会被存入DB
	cachedINMap map[string]bool //当前被缓存在中间节点的非叶子节点,通常由cachedLN分裂而来，会被存入DB

	hotnessList  map[string]int //被访问过的叶子节点bucketKey及其被访频次，不会被存入DB
	accessLength int            //统计MGT中被访问过的所有叶子节点路径长度总和，会被存入DB
}

// NewMGT creates a empty MGT
func NewMGT(rdx int) *MGT {
	return &MGT{rdx, nil, nil, make(map[string]bool), make(map[string]bool), make(map[string]int), 0}
}

// 获取root,如果root为空,则从leveldb中读取
func (mgt *MGT) GetRoot(db *leveldb.DB) *MGTNode {
	if mgt.Root == nil {
		mgtString, error := db.Get(mgt.mgtRootHash, nil)
		if error == nil {
			m, _ := DeserializeMGTNode(mgtString, mgt.rdx)
			mgt.Root = m
		}
	}
	return mgt.Root
}

// 获取subnode,如果subnode为空,则从leveldb中读取
func (mgtNode *MGTNode) GetSubnode(index int, db *leveldb.DB, rdx int) *MGTNode {
	if mgtNode.subNodes[index] == nil {
		nodeString, error := db.Get(mgtNode.dataHashes[index], nil)
		if error == nil {
			node, _ := DeserializeMGTNode(nodeString, rdx)
			mgtNode.subNodes[index] = node
		}
	}
	return mgtNode.subNodes[index]
}

// 获取cachedNode,如果cachedNode为空,则从leveldb中读取
func (mgtNode *MGTNode) GetCachedNode(index int, db *leveldb.DB, rdx int) *MGTNode {
	if mgtNode.cachedNodes[index] == nil {
		nodeString, error := db.Get(mgtNode.cachedDataHashes[index], nil)
		if error == nil {
			node, _ := DeserializeMGTNode(nodeString, rdx)
			mgtNode.cachedNodes[index] = node
		}
	}
	return mgtNode.cachedNodes[index]
}

// 获取bucket,如果bucket为空,则从leveldb中读取
func (mgtNode *MGTNode) GetBucket(rdx int, name string, db *leveldb.DB) *Bucket {
	if mgtNode.bucket == nil {
		bucketString, error := db.Get([]byte(name+"bucket"+util.IntArrayToString(mgtNode.bucketKey, rdx)), nil)
		if error == nil {
			bucket, _ := DeserializeBucket(bucketString)
			mgtNode.bucket = bucket
		}
	}
	return mgtNode.bucket
}

// 更新mgtRootHash,并将mgt存入leveldb
func (mgt *MGT) UpdateMGTToDB(db *leveldb.DB) []byte {
	//get the old mgtHash
	hash := sha256.Sum256(mgt.mgtRootHash)
	oldMgtHash := hash[:]
	//delete the old mgt in leveldb
	db.Delete(oldMgtHash, nil)
	// update mgtRootHash
	mgt.mgtRootHash = mgt.GetRoot(db).nodeHash
	//insert mgt in leveldb
	hash = sha256.Sum256(mgt.mgtRootHash)
	mgtHash := hash[:]
	db.Put(mgtHash, SerializeMGT(mgt), nil)
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
		mgtNode = &MGTNode{nodeHash, subNodes, dataHashes, make([]*MGTNode, rdx), make([][]byte, rdx), isLeaf, nil, subNodes[0].bucketKey[1:]}
	} else {
		mgtNode = &MGTNode{nodeHash, subNodes, dataHashes, nil, nil, isLeaf, bucket, bucket.bucketKey}
	}
	//将mgtNode存入leveldb
	nodeString := SerializeMGTNode(mgtNode)
	db.Put(nodeHash, nodeString, nil)

	return mgtNode
}

// 更新nodeHash,并将node存入leveldb
func (mgtNode *MGTNode) UpdateMGTNodeToDB(db *leveldb.DB) {
	// //delete the old node in leveldb
	// db.Delete(mgtNode.nodeHash, nil)
	//update nodeHash
	UpdateNodeHash(mgtNode)
	//insert node in leveldb
	// fmt.Printf("When write MGTNode to DB, mgtNode.nodeHash: %x\n", mgtNode.nodeHash)
	db.Put(mgtNode.nodeHash, SerializeMGTNode(mgtNode), nil)
}

// 根据bucketKey,返回该bucket在MGT中的叶子节点,第0个是叶节点,最后一个是根节点
func (mgt *MGT) GetLeafNodeAndPath(bucketKey []int, db *leveldb.DB) []*MGTNode {
	result := make([]*MGTNode, 0)
	//递归遍历根节点的所有子节点,找到bucketKey对应的叶子节点
	p := mgt.GetRoot(db)
	//将p插入到result的第0个位置
	result = append(result, p)
	if len(bucketKey) == 0 {
		//更新叶子节点的访问频次
		mgt.UpdateHotnessList("add", util.IntArrayToString(result[0].bucketKey, mgt.rdx), 1, nil)
		//更新LN访问路径总长度
		mgt.UpdateAccessLengthSum(len(result))
		return result
	}
	//从根节点开始,逐层向下遍历,直到找到叶子节点
	//如果该bucketKey对应的叶子节点未被缓存，则一直在subNodes下找，否则需要去cachedNodes里找
	if !mgt.cachedLNMap[util.IntArrayToString(bucketKey, mgt.rdx)] {
		for identI := len(bucketKey) - 1; identI >= 0; identI-- {
			if p == nil {
				return nil
			}
			if p.isLeaf { // 当所找bucketKey的长度长于MGT中实际存在的路径，即bucket并不存在时，返回nil
				return nil
			}
			p = p.GetSubnode(bucketKey[identI], db, mgt.rdx)
			//将p插入到result的第0个位置
			result = append([]*MGTNode{p}, result...)
		}
		//更新叶子节点的访问频次
		mgt.UpdateHotnessList("add", util.IntArrayToString(result[0].bucketKey, mgt.rdx), 1, nil)
		//更新LN访问路径总长度
		mgt.UpdateAccessLengthSum(len(result))
		return result
	} else {
		for identI := len(bucketKey) - 1; identI >= 0; identI-- {
			//获取当前节点的第identI号缓存子节点
			cachedNode := p.GetCachedNode(bucketKey[identI], db, mgt.rdx)
			//判断缓存子节点是否是叶子
			if cachedNode.isLeaf {
				// 如果是叶子则比较是否与要找的bucketKey相同：相同则返回结果；不相同则p移动到第identI个子节点，切换为下一个identI。
				if util.IntArrayToString(cachedNode.bucketKey, mgt.rdx) == util.IntArrayToString(bucketKey, mgt.rdx) {
					//将缓存叶子节点插入到result的第0个位置
					result = append([]*MGTNode{cachedNode}, result...)
					//更新叶子节点的访问频次
					mgt.UpdateHotnessList("add", util.IntArrayToString(result[0].bucketKey, mgt.rdx), 1, nil)
					//更新LN访问路径总长度
					mgt.UpdateAccessLengthSum(len(result))
					return result
				} else {
					p = p.GetSubnode(bucketKey[identI], db, mgt.rdx)
					//将p插入到result的第0个位置
					result = append([]*MGTNode{p}, result...)
				}
			} else {
				//如果不是叶子节点，则判断要找的bucketKey是否包含当前缓存中间节点的bucketKey
				//包含则从当前缓存中间节点开始向下找subNode
				if strings.HasSuffix(util.IntArrayToString(bucketKey, mgt.rdx), util.IntArrayToString(cachedNode.bucketKey, mgt.rdx)) {
					//将cachedNode加入结果列表中
					result = append([]*MGTNode{cachedNode}, result...)
					//p指向cachedNode，并递归其SubNode
					p = cachedNode
					for identI = identI - 1; identI >= 0; identI-- {
						p = p.GetSubnode(bucketKey[identI], db, mgt.rdx)
						//将p插入到result的第0个位置
						result = append([]*MGTNode{p}, result...)
					}
					//更新叶子节点的访问频次
					mgt.UpdateHotnessList("add", util.IntArrayToString(result[0].bucketKey, mgt.rdx), 1, nil)
					//更新LN访问路径总长度
					mgt.UpdateAccessLengthSum(len(result))
					return result
				} else { //不包含则p移动到第identI个子节点，切换为下一个identI继续查找
					p = p.GetSubnode(bucketKey[identI], db, mgt.rdx)
					result = append([]*MGTNode{p}, result...)
				}
			}
		}
	}
	//更新叶子节点的访问频次
	mgt.UpdateHotnessList("add", util.IntArrayToString(result[0].bucketKey, mgt.rdx), 1, nil)
	//更新LN访问路径总长度
	mgt.UpdateAccessLengthSum(len(result))
	return result
}

// 根据bucketKey,返回该中间节点及其在MGT中的访问路径,第0个是该中间节点,最后一个是根节点
func (mgt *MGT) GetInternalNodeAndPath(bucketKey []int, db *leveldb.DB) []*MGTNode {
	result := make([]*MGTNode, 0)
	//递归遍历根节点的所有子节点,找到bucketKey对应的中间节点
	p := mgt.GetRoot(db)
	//将p插入到result的第0个位置
	result = append(result, p)
	if len(bucketKey) == 0 {
		return result
	}
	//从根节点开始,逐层向下遍历,直到找到该中间节点
	//如果该bucketKey对应的中间节点未被缓存，则一直在subNodes下找，否则需要去cachedNodes里找
	if !mgt.cachedINMap[util.IntArrayToString(bucketKey, mgt.rdx)] {
		for identI := len(bucketKey) - 1; identI >= 0; identI-- {
			if p == nil {
				return nil
			}
			if p.isLeaf { // 当所找bucketKey的长度长于MGT中实际存在的路径，即bucket并不存在时，返回nil
				return nil
			}
			p = p.GetSubnode(bucketKey[identI], db, mgt.rdx)
			//将p插入到result的第0个位置
			result = append([]*MGTNode{p}, result...)
		}
		return result
	} else {
		for identI := len(bucketKey) - 1; identI >= 0; identI-- {
			//获取当前节点的第identI号缓存子节点
			cachedNode := p.GetCachedNode(identI, db, mgt.rdx)
			//判断缓存子节点是否是中间节点
			if !cachedNode.isLeaf {
				// 如果是要找的中间节点
				if util.IntArrayToString(cachedNode.bucketKey, mgt.rdx) == util.IntArrayToString(bucketKey, mgt.rdx) {
					result = append([]*MGTNode{cachedNode}, result...)
					return result
				} else if strings.HasSuffix(util.IntArrayToString(bucketKey, mgt.rdx), util.IntArrayToString(cachedNode.bucketKey, mgt.rdx)) {
					//如果要找的中间节点的bucketKey以该缓存中间节点的bucketKey为后缀,则继续搜索其子节点
					//将cachedNode加入结果列表中
					result = append([]*MGTNode{cachedNode}, result...)
					//p指向cachedNode，并递归其SubNode
					p = cachedNode
					for identI = identI - 1; identI >= 0; identI-- {
						p = p.GetSubnode(bucketKey[identI], db, mgt.rdx)
						//将p插入到result的第0个位置
						result = append([]*MGTNode{p}, result...)
					}
					return result
				} else {
					//如果不在该缓存目录下,则p移动到第identI个子节点，切换为下一个identI继续查找
					p = p.GetSubnode(bucketKey[identI], db, mgt.rdx)
					result = append([]*MGTNode{p}, result...)
				}
			}
		}
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
func (mgt *MGT) MGTUpdate(newBuckets []*Bucket, db *leveldb.DB) *MGT {
	if len(newBuckets) == 0 {
		fmt.Printf("newBuckets is empty\n")
		return mgt
	}
	//如果root为空,则直接为newBuckets创建叶节点(newBuckets中只有一个bucket)
	if mgt.GetRoot(db) == nil {
		mgt.Root = NewMGTNode(nil, true, newBuckets[0], db, newBuckets[0].rdx)
		mgt.Root.UpdateMGTNodeToDB(db)
		//统计访问频次
		mgt.UpdateHotnessList("new", util.IntArrayToString(mgt.Root.bucketKey, mgt.rdx), 1, nil)
		return mgt
	}
	var nodePath []*MGTNode
	//如果newBuckets中只有一个bucket，则说明没有发生分裂，只更新nodePath中所有的哈希值
	if len(newBuckets) == 1 {
		nodePath = mgt.GetLeafNodeAndPath(newBuckets[0].bucketKey, db)
		//更新叶子节点的dataHashes
		nodePath[0].dataHashes = make([][]byte, 0)
		for _, merkleTree := range newBuckets[0].GetMerkleTrees() {
			nodePath[0].dataHashes = append(nodePath[0].dataHashes, merkleTree.GetRootHash())
		}
		//更新叶子节点的nodeHash,并将叶子节点存入leveldb
		nodePath[0].UpdateMGTNodeToDB(db)
		//更新所有父节点的nodeHashs,并将父节点存入leveldb
		for i := 1; i < len(nodePath); i++ {
			nodePath[i].dataHashes[newBuckets[0].bucketKey[i-1]] = nodePath[i-1].nodeHash
			nodePath[i].UpdateMGTNodeToDB(db)
		}
	} else {
		//如果newBuckets中有多个bucket，则说明发生了分裂，MGT需要生长
		oldBucketKey := GetOldBucketKey(newBuckets[0])
		// fmt.Printf("oldBucketKey: %s\n", util.IntArrayToString(oldBucketKey, mgt.rdx))
		//根据旧bucketKey,找到旧bucket所在的叶子节点
		nodePath = mgt.GetLeafNodeAndPath(oldBucketKey, db)
		mgt.MGTGrow(oldBucketKey, nodePath, newBuckets, db)
	}
	return mgt
}

// MGT生长,给定旧bucketKey和新的buckets,返回更新后的MGT
func (mgt *MGT) MGTGrow(oldBucketKey []int, nodePath []*MGTNode, newBuckets []*Bucket, db *leveldb.DB) *MGT {
	//为每个新的bucket创建叶子节点,并插入到leafNode的subNodes中
	subNodes := make([]*MGTNode, 0)

	for i := 0; i < len(newBuckets); i++ {
		newNode := NewMGTNode(nil, true, newBuckets[i], db, newBuckets[0].rdx)
		subNodes = append(subNodes, newNode)
		newNode.UpdateMGTNodeToDB(db)
	}

	//更新hotnesslist，更新叶子节点的访问频次
	mgt.UpdateHotnessList("split", util.IntArrayToString(oldBucketKey, mgt.rdx), 0, subNodes)

	//创建新的父节点
	newFatherNode := NewMGTNode(subNodes, false, nil, db, newBuckets[0].rdx)
	newFatherNode.UpdateMGTNodeToDB(db)

	//如果当前分裂的节点是缓存节点,则需要将其分裂出的子节点放入缓存叶子节点列表中,该节点放入缓存中间节点列表中
	if len(oldBucketKey) > len(nodePath) {
		delete(mgt.cachedLNMap, util.IntArrayToString(oldBucketKey, mgt.rdx))
		mgt.cachedINMap[util.IntArrayToString(oldBucketKey, mgt.rdx)] = true
		for _, node := range subNodes {
			mgt.cachedLNMap[util.IntArrayToString(node.bucketKey, mgt.rdx)] = true
		}
	}

	//更新父节点的chid为新的父节点
	if len(nodePath) == 1 {
		mgt.Root = newFatherNode
		mgt.Root.UpdateMGTNodeToDB(db)
		return mgt
	}
	nodePath[1].subNodes[oldBucketKey[0]] = newFatherNode
	nodePath[1].UpdateMGTNodeToDB(db)

	//更新所有父节点的nodeHash
	for i := 2; i < len(nodePath); i++ {
		nodePath[i].dataHashes[oldBucketKey[i-1]] = nodePath[i-1].nodeHash
		nodePath[i].UpdateMGTNodeToDB(db)
	}
	return mgt
}

// 根据子节点哈希和缓存子节点哈希计算当前节点哈希
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

	var keys2 []int
	for key := range node.cachedDataHashes {
		keys2 = append(keys2, key)
	}
	sort.Slice(keys2, func(i int, j int) bool {
		return i < j
	})
	for _, key := range keys2 {
		nodeHash = append(nodeHash, node.cachedDataHashes[key]...)
	}
	hash := sha256.Sum256(nodeHash)
	node.nodeHash = hash[:]
}

// 访问频次统计
// 更新频次列表:
// op是操作类型，包括new,add,split，
// bk是待操作的叶节点bucketKey的string形式,hn是对应要增加的热度,一般为1
// 当op为split时，bk是旧叶子节点的bucketKey，subnodes是新生成的子叶子节点列表
func (mgt *MGT) UpdateHotnessList(op string, bk string, hn int, subnodes []*MGTNode) {
	if op == "new" {
		mgt.hotnessList[bk] = hn
		// fmt.Println("new hotness of bucketKey=", bk, "is", mgt.hotnessList[bk])
	} else if op == "add" {
		mgt.hotnessList[bk] = mgt.hotnessList[bk] + hn
		// fmt.Println("add hotness of bucketKey=", bk, "is", mgt.hotnessList[bk])
	} else if op == "split" {
		originhot := mgt.hotnessList[bk] + hn
		record_sum := 0
		for _, node := range subnodes {
			record_sum += node.bucket.number
			// fmt.Println("subnodeNum=", node.bucket.number)
		}
		// fmt.Println("record_sum=", record_sum)
		for _, node := range subnodes {
			subhotness := int(float64(originhot) * (float64(node.bucket.number) / float64(record_sum)))
			if subhotness != 0 {
				mgt.hotnessList[util.IntArrayToString(node.bucketKey, mgt.rdx)] = subhotness
				// fmt.Println("add hotness of subbucketKey=", util.IntArrayToString(node.bucketKey, mgt.rdx), "is", mgt.hotnessList[util.IntArrayToString(node.bucketKey, mgt.rdx)])
			}
		}
		delete(mgt.hotnessList, bk)
	} else {
		fmt.Println("Error: no op!")
	}
}

// 更新叶子节点访问路径总长度
func (mgt *MGT) UpdateAccessLengthSum(accessPath int) {
	mgt.accessLength = mgt.accessLength + accessPath
}

// 计算是否需要调整缓存
// a和b是计算阈值的两个参数
func (mgt *MGT) IsNeedCacheAdjust(bucketNum int, a float64, b float64) bool {
	//计算总访问频次
	accessNum := 0
	hotnessSlice := util.SortStringIntMapByInt(&mgt.hotnessList)
	for i := 0; i < len(hotnessSlice); i++ {
		accessNum = accessNum + hotnessSlice[i].GetValue()
	}
	//计算访问长度阈值
	// threshold := (a*math.Log(float64(bucketNum))/math.Log(float64(mgt.rdx)) + b*math.Log(float64(bucketNum))/math.Log(float64(mgt.rdx))) * float64(accessNum)
	threshold := (a*math.Log(float64(bucketNum)/float64(10))/math.Log(float64(mgt.rdx)) + b*math.Log(float64(bucketNum))/math.Log(float64(mgt.rdx))) * float64(accessNum)

	fmt.Println("threshold =", threshold)

	return float64(mgt.accessLength) > threshold
}

// 调整缓存
func (mgt *MGT) CacheAdjust(db *leveldb.DB) []byte {
	//第一步: 先将所有非叶子节点放回原处
	for INode, _ := range mgt.cachedINMap {
		//找到其所在的路径
		bk_INode, _ := util.StringToIntArray(INode, mgt.rdx)
		nodePath := mgt.GetInternalNodeAndPath(bk_INode, db)
		//将其所在的缓存父节点(nodePath[1])相应的缓存位置空
		fmt.Println(bk_INode)
		mgt.PrintCachedPath(nodePath)
		tempBK := bk_INode[:len(bk_INode)-len(nodePath[1].bucketKey)]
		nodePath[1].cachedNodes[tempBK[len(tempBK)-1]] = nil
		nodePath[1].cachedDataHashes[tempBK[len(tempBK)-1]] = nil
		//将该INode在cachedINMap中删除
		delete(mgt.cachedINMap, INode)
		//将以INode为后缀的所有cachedLNMap中删除
		for key, _ := range mgt.cachedLNMap {
			if strings.HasSuffix(key, INode) {
				delete(mgt.cachedLNMap, key)
			}
		}
		//获取其父节点
		nodePath_father := mgt.GetInternalNodeAndPath(bk_INode[1:], db)
		//将其父节点的相应孩子位置为该节点
		nodePath_father[0].subNodes[bk_INode[0]] = nodePath[0]
		nodePath_father[0].dataHashes[bk_INode[0]] = nodePath[0].nodeHash
		//更新nodePath_father中所有节点的nodeHash(nodePath_father一定包含nodePath中的所有节点,因此无须再更新nodePath)
		nodePath_father[0].UpdateMGTNodeToDB(db)
		for i := 1; i < len(nodePath_father); i++ {
			nodePath_father[i].dataHashes[bk_INode[i]] = nodePath_father[i-1].nodeHash
			nodePath_father[i].UpdateMGTNodeToDB(db)
		}
	}
	//将缓存的中间节点清空
	mgt.cachedINMap = make(map[string]bool)

	//第二步: 从hotnessList中依次选择最高的访问桶进行放置
	hotnessSlice := util.SortStringIntMapByInt(&mgt.hotnessList)
	for j := 0; j < len(hotnessSlice); j++ {
		// hotnessSlice[j].PrintKV()
		bucketKey, _ := util.StringToIntArray(hotnessSlice[j].GetKey(), mgt.rdx)
		//找到当前叶子节点及其路径
		nodePath := mgt.GetLeafNodeAndPath(bucketKey, db)
		if len(nodePath) <= 2 {
			//说明该叶子节点是根节点或者该叶子节点在根节点的缓存目录中,已经没有缓存优化空间
			fmt.Println("已是叶节点", hotnessSlice[j].GetKey(), "的最佳位置")
			mgt.PrintCachedPath(nodePath)
			continue
		}
		newPath := make([]*MGTNode, 0)
		//从nodePath的最后一个开始,倒着看是否能够放置
		identI := len(bucketKey) - 1
		for i := len(nodePath) - 1; i > 1; i-- {
			newPath = append([]*MGTNode{nodePath[i]}, newPath...)
			//如果当前节点的第bucketKey[identI]个缓存位为空,则放置
			if nodePath[i].GetCachedNode(bucketKey[identI], db, mgt.rdx) == nil {
				//1.放置当前节点(nodePath[0])
				nodePath[i].cachedNodes[bucketKey[identI]] = nodePath[0]
				newPath = append([]*MGTNode{nodePath[0]}, newPath...)
				nodePath[i].cachedDataHashes[bucketKey[identI]] = nodePath[0].nodeHash
				//2.如果nodePath是缓存路径，则将nodePath[1]相应缓存位清空
				if mgt.cachedLNMap[util.IntArrayToString(bucketKey, mgt.rdx)] {
					tempBK := bucketKey[:len(bucketKey)-len(nodePath[1].bucketKey)]
					nodePath[1].cachedNodes[tempBK[len(tempBK)-1]] = nil
					nodePath[1].cachedDataHashes[tempBK[len(tempBK)-1]] = nil
				}
				//3.更新nodePath[1]及以后的所有节点
				nodePath[1].UpdateMGTNodeToDB(db)
				for i := 2; i < len(nodePath); i++ {
					nodePath[i].dataHashes[nodePath[i-1].bucketKey[0]] = nodePath[i-1].nodeHash
					nodePath[i].UpdateMGTNodeToDB(db)
				}
				//4.将该叶子节点放入缓存Map中
				mgt.cachedLNMap[hotnessSlice[j].GetKey()] = true
				//5.打印缓存更新结果
				mgt.PrintCachedPath(newPath)
				break
			} else { //如果当前节点的第bucketKey[identI]个缓存位不为空
				//比较缓存节点与当前叶节点的热度,如果当前节点更热,则替换,并将原缓存节点放回原处,否则继续看下一位置
				cachedLN := nodePath[i].GetCachedNode(bucketKey[identI], db, mgt.rdx)
				//待插入节点更热
				if mgt.hotnessList[util.IntArrayToString(cachedLN.bucketKey, mgt.rdx)] == 0 || mgt.hotnessList[util.IntArrayToString(cachedLN.bucketKey, mgt.rdx)] < mgt.hotnessList[util.IntArrayToString(nodePath[0].bucketKey, mgt.rdx)] {
					//1.先将cachedLN放回原处
					//在cachedMap中删除
					delete(mgt.cachedLNMap, util.IntArrayToString(cachedLN.bucketKey, mgt.rdx))
					//找到cachedLN的父节点
					np_father := mgt.GetLeafNodeAndPath(cachedLN.bucketKey[1:], db)
					//将cachedLN放入其父节点的子节点中
					np_father[0].subNodes[cachedLN.bucketKey[0]] = cachedLN
					np_father[0].dataHashes[cachedLN.bucketKey[0]] = cachedLN.nodeHash
					//2.更新np_father上所有节点
					np_father[0].UpdateMGTNodeToDB(db)
					for i := 1; i < len(np_father); i++ {
						np_father[i].dataHashes[np_father[i-1].bucketKey[0]] = np_father[i-1].nodeHash
						np_father[i].UpdateMGTNodeToDB(db)
					}
					//3.再将当前节点放入nodePath[i]的缓存目录中
					nodePath[i].cachedNodes[bucketKey[identI]] = nodePath[0]
					nodePath[i].cachedDataHashes[bucketKey[identI]] = nodePath[0].nodeHash
					newPath = append([]*MGTNode{nodePath[0]}, newPath...)
					//4.如果nodePath是缓存路径，则将nodePath[1]相应缓存位清空
					if mgt.cachedLNMap[util.IntArrayToString(bucketKey, mgt.rdx)] {
						tempBK := bucketKey[:len(bucketKey)-len(nodePath[1].bucketKey)]
						nodePath[1].cachedNodes[tempBK[len(tempBK)-1]] = nil
						nodePath[1].cachedDataHashes[tempBK[len(tempBK)-1]] = nil
					}
					//5.更新nodePath[1]及以后的所有节点
					nodePath[1].UpdateMGTNodeToDB(db)
					for i := 2; i < len(nodePath); i++ {
						nodePath[i].dataHashes[nodePath[i-1].bucketKey[0]] = nodePath[i-1].nodeHash
						nodePath[i].UpdateMGTNodeToDB(db)
					}
					//6.将该节点放入缓存Map中
					mgt.cachedLNMap[hotnessSlice[j].GetKey()] = true
					//7.打印缓存更新结果
					mgt.PrintCachedPath(newPath)
					break
				} else {
					//原缓存节点更热
					identI--
				}
			}
			if i == 2 {
				if !mgt.cachedLNMap[util.IntArrayToString(bucketKey, mgt.rdx)] {
					fmt.Println("叶节点", util.IntArrayToString(bucketKey, mgt.rdx), "未被缓存")
				} else {
					fmt.Println("叶节点", util.IntArrayToString(bucketKey, mgt.rdx), "缓存路径未改变")
				}
				mgt.PrintCachedPath(nodePath)
			}
		}
	}

	//第三步: 清空统计列表
	mgt.hotnessList = make(map[string]int)
	mgt.accessLength = 0

	return mgt.UpdateMGTToDB(db)
}

// 打印缓存更新后的缓存路径
func (mgt *MGT) PrintCachedPath(cachedPath []*MGTNode) {
	fmt.Print("缓存路径【", len(cachedPath), "】：")
	for i := 0; i < len(cachedPath); i++ {
		if util.IntArrayToString(cachedPath[i].bucketKey, mgt.rdx) == "" {
			fmt.Print("Root")
		} else {
			fmt.Print(util.IntArrayToString(cachedPath[i].bucketKey, mgt.rdx))
			fmt.Print("--->")
		}
	}
	fmt.Println()
}

// 打印缓存情况
func (mgt *MGT) PrintCachedMaps() {
	fmt.Println("cachedLNMap:", mgt.cachedLNMap)
	fmt.Println("cachedINMap:", mgt.cachedINMap)
}

// 打印访问频次列表和总访问次数
func (mgt *MGT) PrintHotnessList() {
	accessNum := 0
	hotnessSlice := util.SortStringIntMapByInt(&mgt.hotnessList)
	for i := 0; i < len(hotnessSlice); i++ {
		hotnessSlice[i].PrintKV()
		accessNum = accessNum + hotnessSlice[i].GetValue()
	}
	fmt.Println("accessNum: ", accessNum)
}

func (mgt *MGT) GetHotnessList() *map[string]int {
	return &mgt.hotnessList
}

func (mgt *MGT) GetAccessLength() int {
	return mgt.accessLength
}

// 打印MGT
func (mgt *MGT) PrintMGT(mehtName string, db *leveldb.DB) {
	fmt.Printf("打印MGT-------------------------------------------------------------------------------------------\n")
	if mgt == nil {
		return
	}
	//递归打印MGT
	fmt.Printf("MGTRootHash: %x\n", mgt.mgtRootHash)
	mgt.PrintMGTNode(mehtName, mgt.GetRoot(db), 0, db)

}

// 递归打印MGT
func (mgt *MGT) PrintMGTNode(mehtName string, node *MGTNode, level int, db *leveldb.DB) {
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
	fmt.Printf("cachedDataHashes:\n")
	for _, cachedDataHash := range node.cachedDataHashes {
		fmt.Printf("%s\n", hex.EncodeToString(cachedDataHash))
	}
	for i := 0; i < len(node.dataHashes); i++ {
		if !node.isLeaf && node.dataHashes[i] != nil {
			mgt.PrintMGTNode(mehtName, node.GetSubnode(i, db, mgt.rdx), level+1, db)
		}
	}
	for i := 0; i < len(node.cachedDataHashes); i++ {
		if !node.isLeaf && node.cachedDataHashes[i] != nil {
			mgt.PrintMGTNode(mehtName, node.GetCachedNode(i, db, mgt.rdx), level+1, db)
		}
	}
}

type MGTProof struct {
	level    int    //哈希值所在的层数
	dataHash []byte //哈希值
}

// 给定bucketKey，返回它的mgtRootHash和mgtProof，不存在则返回nil
func (mgt *MGT) GetProof(bucketKey []int, db *leveldb.DB) ([]byte, []MGTProof) {
	//根据bucketKey,找到叶子节点和路径
	nodePath := mgt.GetLeafNodeAndPath(bucketKey, db)
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
		for j := 0; j < len(nodePath[i].cachedDataHashes); j++ {
			mgtProof = append(mgtProof, MGTProof{i, nodePath[i].cachedDataHashes[j]})
		}
	}
	return mgt.Root.nodeHash, mgtProof
}

// 给定segRootHash和mgtProof，返回由它们计算得到的mgtRootHash
func ComputMGTRootHash(segRootHash []byte, mgtProof []MGTProof) []byte {
	//遍历mgtProof中前segNum个元素，如果segRootHash不存在，则返回nil，否则计算得到第0个node的nodeHash
	//同样遍历第i层的所有元素，如果第i-1层的nodehash不在其中，则返回nil，否则计算得到第i层node的nodeHash
	isSRHExist := false
	nodeHash0 := segRootHash
	var nodeHash1 []byte
	level := 0
	for i := 0; i <= len(mgtProof)-1; i++ {
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

type SeMGT struct {
	Rdx         int    //radix of bucket key, decide the number of sub-nodes
	MgtRootHash []byte // hash of this MGT, equals to the hash of the root node hash

	CachedLNMapstring string //当前被缓存在中间节点的叶子节点，存在bool为true，不存在就直接没有
	CachedINMapstring string //当前被缓存在中间节点的叶子节点，存在bool为true，不存在就直接没有
}

func SerializeMGT(mgt *MGT) []byte {
	//将cachedLNMap合并为一个string
	cachedLNMapstring := ""
	for k := range mgt.cachedLNMap {
		cachedLNMapstring = cachedLNMapstring + "," + k
	}
	//将cachedINMap合并为一个string
	cachedINMapstring := ""
	for k := range mgt.cachedINMap {
		cachedINMapstring = cachedINMapstring + "," + k
	}
	seMGT := &SeMGT{mgt.rdx, mgt.mgtRootHash, cachedLNMapstring, cachedINMapstring}
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
	//复原缓存叶子节点Map
	cachedLNMapstrings := strings.Split(seMGT.CachedLNMapstring, ",")
	cachedLNMap := make(map[string]bool)
	for _, value := range cachedLNMapstrings {
		if value != "" {
			cachedLNMap[value] = true
		}
	}
	//复原缓存非叶子节点Map
	cachedINMapstrings := strings.Split(seMGT.CachedINMapstring, ",")
	cachedINMap := make(map[string]bool)
	for _, value := range cachedINMapstrings {
		if value != "" {
			cachedINMap[value] = true
		}
	}
	mgt := &MGT{seMGT.Rdx, nil, seMGT.MgtRootHash, cachedLNMap, cachedINMap, make(map[string]int), 0}
	return mgt, nil
}

type SeMGTNode struct {
	NodeHash   []byte // hash of this node, consisting of the hash of its children
	DataHashes string // hashes of data elements, computed from subNodes, is used for indexing children nodes in leveldb

	CachedDataHashes string // hash of cached data elements, computed from cached subNodes

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
	cachedDataHashString := ""
	for i := 0; i < len(node.cachedDataHashes); i++ {
		cachedDataHashString += hex.EncodeToString(node.cachedDataHashes[i])
		if i != len(node.cachedDataHashes)-1 {
			cachedDataHashString += ","
		}
	}
	seMGTNode := &SeMGTNode{node.nodeHash, dataHashString, cachedDataHashString, node.isLeaf, node.bucketKey}
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
	cachedDataHashes := make([][]byte, 0)
	cachedDataHasheStrings := strings.Split(seMGTNode.CachedDataHashes, ",")
	for i := 0; i < len(cachedDataHasheStrings); i++ {
		cachedDataHash, _ := hex.DecodeString(cachedDataHasheStrings[i])
		cachedDataHashes = append(cachedDataHashes, cachedDataHash)
	}
	cachedNodes := make([]*MGTNode, rdx)
	mgtNode := &MGTNode{seMGTNode.NodeHash, subnodes, dataHashes, cachedNodes, cachedDataHashes, seMGTNode.IsLeaf, nil, seMGTNode.BucketKey}
	return mgtNode, nil
}
