package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"math"
	"sort"
	"strings"
	"sync"
	// "MEHT/util"
)

type MGTNode struct {
	nodeHash         []byte     // hash of this node, consisting of the hash of its children
	parent           *MGTNode   // parent node of this node
	subNodes         []*MGTNode // sub-nodes in the tree, original given
	dataHashes       [][]byte   // hashes of data elements, computed from subNodes
	cachedNodes      []*MGTNode // cached nodes
	cachedDataHashes [][]byte   //dataHashes of cached nodes
	//NOTE：如果cached node是中间节点（isLeaf是false），则表明发生了分裂
	isLeaf           bool    // whether this node is a leaf node
	isDirty          bool    // whether the hash of this node needs to be updated
	bucket           *Bucket // bucket related to this leaf node
	bucketKey        []int   // bucket key
	latch            sync.RWMutex
	subNodesLatch    []sync.RWMutex
	cachedNodesLatch []sync.RWMutex
}

type MGT struct {
	rdx         int      //radix of bucket key, decide the number of sub-nodes
	Root        *MGTNode // root node of the tree
	mgtRootHash []byte   // hash of this MGT, equals to the root node hash
	//cachedLNMap map[string]bool
	cachedLNMap sync.Map //当前被缓存在中间节点的叶子节点，存在bool为true，不存在就直接没有，会被存入DB
	//cachedINMap map[string]bool
	cachedINMap  sync.Map       //当前被缓存在中间节点的非叶子节点,通常由cachedLN分裂而来，会被存入DB
	hotnessList  map[string]int //被访问过的叶子节点bucketKey及其被访频次，不会被存入DB
	accessLength int            //统计MGT中被访问过的所有叶子节点路径长度总和，会被存入DB
	latch        sync.RWMutex
	updateLatch  sync.Mutex
}

// NewMGT creates an empty MGT
func NewMGT(rdx int) *MGT {
	return &MGT{rdx, nil, nil, sync.Map{}, sync.Map{}, make(map[string]int), 0, sync.RWMutex{}, sync.Mutex{}}
}

func (mgt *MGT) GetRdx() int {
	return mgt.rdx
}

// GetRoot 获取root,如果root为空,则从leveldb中读取
func (mgt *MGT) GetRoot(db *leveldb.DB) *MGTNode {
	if mgt.Root == nil {
		mgt.updateLatch.Lock() // 由于不一定有root，所以是串行的
		defer mgt.updateLatch.Unlock()
		if mgt.Root != nil { // 可能刚好进到if之后但lock之前root被别的线程更新了
			return mgt.Root
		}
		if mgtString, error_ := db.Get(mgt.mgtRootHash, nil); error_ == nil {
			m, _ := DeserializeMGTNode(mgtString, mgt.rdx)
			mgt.Root = m
			//此处由于root尚未被其他地方引用，因此getBucket无需加锁
			mgt.Root.GetBucket(mgt.rdx, util.IntArrayToString(mgt.Root.bucketKey, mgt.rdx), db, nil)
		}
	}
	return mgt.Root
}

func (mgtNode *MGTNode) GetNodeHash() []byte {
	return mgtNode.nodeHash
}

func (mgtNode *MGTNode) GetParent() *MGTNode {
	return mgtNode.parent
}

func (mgtNode *MGTNode) GetBucketKey() []int {
	return mgtNode.bucketKey
}

func (mgtNode *MGTNode) GetSubNodeHash(i int) []byte {
	return mgtNode.dataHashes[i]
}

func (mgtNode *MGTNode) GetCachedNodeHash(i int) []byte {
	return mgtNode.cachedDataHashes[i]
}

func (mgtNode *MGTNode) GetIsDirty() bool {
	return mgtNode.isDirty
}
func (mgtNode *MGTNode) GetIsLeaf() bool {
	return mgtNode.isLeaf
}

// GetSubNode 获取subNode,如果subNode为空,则从leveldb中读取
func (mgtNode *MGTNode) GetSubNode(index int, db *leveldb.DB, rdx int, cache *[]interface{}) *MGTNode {
	if mgtNode.subNodes[index] == nil && mgtNode.subNodesLatch[index].TryLock() { // 既然进入这个函数那么是一定能找到节点的
		if mgtNode.subNodes[index] != nil {
			mgtNode.subNodesLatch[index].Unlock()
			return mgtNode.subNodes[index]
		}
		var node *MGTNode
		var ok bool
		if cache != nil {
			targetCache, _ := (*cache)[0].(*lru.Cache[string, *MGTNode])
			if node, ok = targetCache.Get(string(mgtNode.dataHashes[index])); ok {
				mgtNode.subNodes[index] = node
				node.parent = mgtNode
			}
		}
		if !ok {
			if nodeString, error_ := db.Get(mgtNode.dataHashes[index], nil); error_ == nil {
				node, _ = DeserializeMGTNode(nodeString, rdx)
				node.parent = mgtNode
				mgtNode.subNodes[index] = node
			}
		}
		if node != nil && node.isLeaf {
			//此处由于node尚未被其他地方引用，因此getBucket无需加锁
			node.GetBucket(rdx, util.IntArrayToString(node.bucketKey, rdx), db, cache)
		}
		mgtNode.subNodesLatch[index].Unlock()
	}
	for mgtNode.subNodes[index] == nil { // 其余线程等待subNode重构
	}
	return mgtNode.subNodes[index]
}

// GetCachedNode 获取cachedNode,如果cachedNode为空,则从leveldb中读取
func (mgtNode *MGTNode) GetCachedNode(index int, db *leveldb.DB, rdx int, cache *[]interface{}) *MGTNode {
	if mgtNode.cachedNodes[index] == nil && len(mgtNode.cachedDataHashes[index]) != 0 && mgtNode.cachedNodesLatch[index].TryLock() { // 既然进入这个函数那么是一定能找到节点的
		if mgtNode.cachedNodes[index] != nil {
			mgtNode.cachedNodesLatch[index].Unlock()
			return mgtNode.cachedNodes[index]
		}
		var node *MGTNode
		var ok bool
		if cache != nil {
			targetCache, _ := (*cache)[0].(*lru.Cache[string, *MGTNode])
			if node, ok = targetCache.Get(string(mgtNode.cachedDataHashes[index])); ok {
				mgtNode.cachedNodes[index] = node
				node.parent = mgtNode
			}
		}
		if !ok {
			if nodeString, error_ := db.Get(mgtNode.cachedDataHashes[index], nil); error_ == nil {
				node, _ = DeserializeMGTNode(nodeString, rdx)
				node.parent = mgtNode
				mgtNode.cachedNodes[index] = node
			}
		}
		if node != nil && node.isLeaf {
			//此处由于node尚未被其他地方引用，因此getBucket无需加锁
			node.GetBucket(rdx, util.IntArrayToString(node.bucketKey, rdx), db, cache)
		}
		mgtNode.cachedNodesLatch[index].Unlock()
	}
	for mgtNode.cachedNodes[index] == nil && len(mgtNode.cachedDataHashes[index]) != 0 { // 其余线程等待subNode重
	}
	return mgtNode.cachedNodes[index]
}

// GetBucket 获取bucket,如果bucket为空,则从leveldb中读取
func (mgtNode *MGTNode) GetBucket(rdx int, name string, db *leveldb.DB, cache *[]interface{}) *Bucket {
	//跳转到此函数时保证无需考虑锁
	if mgtNode.bucket == nil {
		var ok bool
		var bucket *Bucket
		key_ := name + "bucket" + util.IntArrayToString(mgtNode.bucketKey, rdx)
		if cache != nil {
			targetCache, _ := (*cache)[1].(*lru.Cache[string, *Bucket])
			bucket, ok = targetCache.Get(key_)
		}
		if !ok {
			if bucketString, error_ := db.Get([]byte(key_), nil); error_ == nil {
				bucket, _ = DeserializeBucket(bucketString)
			}
		}
		mgtNode.bucket = bucket
	}
	return mgtNode.bucket
}

// UpdateMGTToDB 更新mgtRootHash,并将mgt存入leveldb
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
func NewMGTNode(subNodes []*MGTNode, isLeaf bool, bucket *Bucket, db *leveldb.DB, rdx int, cache *[]interface{}, commonKeyLength ...int) *MGTNode {
	nodeHash := make([]byte, 0)
	dataHashes := make([][]byte, 0)
	//如果是叶子节点,遍历其所有segment,将每个segment的根hash加入dataHashes
	if isLeaf {
		bucket.GetMerkleTrees().Range(func(key, value interface{}) bool {
			dataHashes = append(dataHashes, value.(*mht.MerkleTree).GetRootHash())
			nodeHash = append(nodeHash, value.(*mht.MerkleTree).GetRootHash()...)
			return true
		})
	} else {
		if subNodes == nil {
			subNodes = make([]*MGTNode, rdx)
			dataHashes = make([][]byte, rdx)
		}
		for i := 0; i < len(subNodes); i++ {
			dataHashes = append(dataHashes, subNodes[i].nodeHash)
			nodeHash = append(nodeHash, subNodes[i].nodeHash...)
		}
	}
	//对dataHashes求hash,得到nodeHash
	hash := sha256.Sum256(nodeHash)
	nodeHash = hash[:]
	var mgtNode *MGTNode
	//通过判断是否是叶子节点决定bucket是否需要
	if !isLeaf {
		mgtNode = &MGTNode{nodeHash, nil, subNodes, dataHashes, make([]*MGTNode, rdx), make([][]byte, rdx), isLeaf, false, nil, subNodes[0].bucketKey[1:], sync.RWMutex{}, make([]sync.RWMutex, 0), make([]sync.RWMutex, 0)}
		for i := 0; i < rdx; i++ {
			mgtNode.subNodesLatch = append(mgtNode.subNodesLatch, sync.RWMutex{})
			mgtNode.cachedNodesLatch = append(mgtNode.cachedNodesLatch, sync.RWMutex{})
		}
	} else {
		pos := 0
		if len(commonKeyLength) > 0 {
			pos = len(bucket.BucketKey) - commonKeyLength[0]
		}
		mgtNode = &MGTNode{nodeHash, nil, subNodes, dataHashes, nil, nil, isLeaf, false, bucket, bucket.BucketKey[pos:], sync.RWMutex{}, nil, nil}
	}
	for _, node := range subNodes {
		if node != nil {
			node.parent = mgtNode
		}
	}
	//将mgtNode存入leveldb
	if cache != nil {
		targetCache, _ := (*cache)[0].(*lru.Cache[string, *MGTNode])
		targetCache.Add(string(nodeHash), mgtNode)
	} else {
		nodeString := SerializeMGTNode(mgtNode)
		if err := db.Put(nodeHash, nodeString, nil); err != nil {
			panic(err)
		}
	}
	return mgtNode
}

// UpdateMGTNodeToDB 更新nodeHash,并将node存入leveldb
func (mgtNode *MGTNode) UpdateMGTNodeToDB(db *leveldb.DB, cache *[]interface{}) {
	//跳转到此函数时MGT已加写锁
	//delete the old node in leveldb
	var targetCache *lru.Cache[string, *MGTNode]
	if cache != nil {
		targetCache, _ = (*cache)[0].(*lru.Cache[string, *MGTNode])
		targetCache.Remove(string(mgtNode.nodeHash))
	}
	if err := db.Delete(mgtNode.nodeHash, nil); err != nil {
		panic(err)
	}
	UpdateNodeHash(mgtNode)
	if cache != nil {
		targetCache.Add(string(mgtNode.nodeHash), mgtNode)
	} else {
		if err := db.Put(mgtNode.nodeHash, SerializeMGTNode(mgtNode), nil); err != nil {
			panic(err)
		}
	}
}

// GetLeafNodeAndPath 根据bucketKey,返回该bucket在MGT中的叶子节点,第0个是叶节点,最后一个是根节点
func (mgt *MGT) GetLeafNodeAndPath(bucketKey []int, db *leveldb.DB, cache *[]interface{}) []*MGTNode {
	// 这个函数会被多个线程同时调用，但是每个线程的完整path一定是不同的，这是因为不同的线程修改的桶一定是不同的，
	// 因此影响到的mgtNode也是不同的，而这些node都会是叶子节点而不是中间节点，因此一个操作自始至终path应该都是不变的，因此mgt的遍历不需要加锁
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
	if val, ok := mgt.cachedLNMap.Load(util.IntArrayToString(bucketKey, mgt.rdx)); !ok || !val.(bool) {
		for identI := len(bucketKey) - 1; identI >= 0; identI-- {
			if p == nil {
				return nil
			}
			p = p.GetSubNode(bucketKey[identI], db, mgt.rdx, cache)
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
			cachedNode := p.GetCachedNode(bucketKey[identI], db, mgt.rdx, cache)
			if cachedNode != nil {
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
					}
				} else {
					//如果不是叶子节点，则判断要找的bucketKey是否包含当前缓存中间节点的bucketKey
					//包含则从当前缓存中间节点开始向下找subNode
					if strings.HasSuffix(util.IntArrayToString(bucketKey, mgt.rdx), util.IntArrayToString(cachedNode.bucketKey, mgt.rdx)) {
						//将cachedNode加入结果列表中
						result = append([]*MGTNode{cachedNode}, result...)
						//p指向cachedNode，并递归其SubNode
						p = cachedNode
						for identI = len(bucketKey) - len(cachedNode.bucketKey) - 1; identI >= 0; identI-- {
							p = p.GetSubNode(bucketKey[identI], db, mgt.rdx, cache)
							//将p插入到result的第0个位置
							result = append([]*MGTNode{p}, result...)
						}
						//更新叶子节点的访问频次
						mgt.UpdateHotnessList("add", util.IntArrayToString(result[0].bucketKey, mgt.rdx), 1, nil)
						//更新LN访问路径总长度
						mgt.UpdateAccessLengthSum(len(result))
						return result
					}
				}
			}
			p = p.GetSubNode(bucketKey[identI], db, mgt.rdx, cache)
			result = append([]*MGTNode{p}, result...)
		}
	}
	//更新叶子节点的访问频次
	mgt.UpdateHotnessList("add", util.IntArrayToString(result[0].bucketKey, mgt.rdx), 1, nil)
	//更新LN访问路径总长度
	mgt.UpdateAccessLengthSum(len(result))
	return result
}

// GetInternalNodeAndPath 根据bucketKey,返回该中间节点及其在MGT中的访问路径,第0个是该中间节点,最后一个是根节点
func (mgt *MGT) GetInternalNodeAndPath(bucketKey []int, db *leveldb.DB, cache *[]interface{}) []*MGTNode {
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
	if val, ok := mgt.cachedINMap.Load(util.IntArrayToString(bucketKey, mgt.rdx)); !ok || !val.(bool) {
		for identI := len(bucketKey) - 1; identI >= 0; identI-- {
			if p == nil {
				return nil
			}
			p = p.GetSubNode(bucketKey[identI], db, mgt.rdx, cache)
			//将p插入到result的第0个位置
			result = append([]*MGTNode{p}, result...)
		}
		return result
	} else {
		for identI := len(bucketKey) - 1; identI >= 0; identI-- {
			//获取当前节点的第identI号缓存子节点
			cachedNode := p.GetCachedNode(bucketKey[identI], db, mgt.rdx, cache)
			if cachedNode != nil {
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
						for identI = len(bucketKey) - len(cachedNode.bucketKey) - 1; identI >= 0; identI-- {
							p = p.GetSubNode(bucketKey[identI], db, mgt.rdx, cache)
							//将p插入到result的第0个位置
							result = append([]*MGTNode{p}, result...)
						}
						return result
					}
				}
			}
			//如果不在该缓存目录下,则p移动到第identI个子节点，切换为下一个identI继续查找
			p = p.GetSubNode(bucketKey[identI], db, mgt.rdx, cache)
			result = append([]*MGTNode{p}, result...)
		}
	}
	return result
}

// GetOldBucketKey 给定一个bucket,返回它的旧bucketKey
func GetOldBucketKey(bucket *Bucket) []int {
	bucketKey := bucket.GetBucketKey()
	if len(bucketKey) == 0 {
		return nil
	}
	oldBucketKey := make([]int, len(bucketKey)-1)
	copy(oldBucketKey, bucketKey[1:])
	return oldBucketKey
}

// MGTBatchFix 对根节点的所有孩子节点做并发，调用递归函数更新脏节点
func (mgt *MGT) MGTBatchFix(db *leveldb.DB, cache *[]interface{}) {
	if mgt.Root == nil || !mgt.Root.isDirty {
		return
	}
	wG := sync.WaitGroup{}
	// 仅对第一层孩子节点做并发处理，减少协程数量
	// 先通过递归到最深层，再从最深层开始往上更新脏节点
	for i, child := range mgt.Root.subNodes {
		if child == nil || !child.isDirty { // child 不存在的节点一定不会是脏节点
			continue
		}
		wG.Add(1)
		child_ := child
		go func(idx int) {
			MGTBatchFixFoo(child_, db, cache)
			mgt.Root.dataHashes[idx] = child_.nodeHash
			wG.Done()
		}(i)
	}
	for i, child := range mgt.Root.cachedNodes {
		if child == nil || !child.isDirty { // child 不存在的节点一定不会是脏节点
			continue
		}
		wG.Add(1)
		child_ := child
		go func(idx int) {
			MGTBatchFixFoo(child_, db, cache)
			mgt.Root.cachedDataHashes[idx] = child_.nodeHash
			wG.Done()
		}(i)
	}
	wG.Wait()
	// 最后更新根节点
	mgt.Root.UpdateMGTNodeToDB(db, cache)
	mgt.Root.isDirty = false
}

// MGTBatchFixFoo 递归更新脏节点
func MGTBatchFixFoo(mgtNode *MGTNode, db *leveldb.DB, cache *[]interface{}) {
	if mgtNode == nil || !mgtNode.isDirty {
		return
	}
	for idx, child := range mgtNode.subNodes {
		if child == nil || !child.isDirty || child.parent != mgtNode { // child 不存在的节点一定不会是脏节点
			continue
		}
		MGTBatchFixFoo(child, db, cache)
		mgtNode.dataHashes[idx] = child.nodeHash
	}
	for idx, child := range mgtNode.cachedNodes {
		if child == nil || !child.isDirty { // child 不存在的节点一定不会是脏节点
			continue
		}
		MGTBatchFixFoo(child, db, cache)
		mgtNode.cachedDataHashes[idx] = child.nodeHash
	}
	mgtNode.UpdateMGTNodeToDB(db, cache)
	mgtNode.isDirty = false
}

// MGTUpdate MGT生长,给定新的buckets,返回更新后的MGT
func (mgt *MGT) MGTUpdate(newBucketSs [][]*Bucket, db *leveldb.DB, cache *[]interface{}) {
	if mgt.Root == nil {
		return
	}
	// 如果newBucketSs是nil，说明是批量提交，对所有脏节点进行哈希重新计算，mgtRootHash也会被更新
	if newBucketSs == nil {
		mgt.MGTBatchFix(db, cache)
		return
	}
	var nodePath []*MGTNode
	//如果newBuckets中只有一个bucket，则说明没有发生分裂，只更新nodePath中所有的哈希值
	//连环分裂至少需要第一层有rdx个桶，因此只需要判断第一层是不是一个桶就知道bucketSs里是不是只有一个桶
	if len(newBucketSs[0]) == 1 {
		bk := newBucketSs[0][0]
		nodePath = mgt.GetLeafNodeAndPath(bk.BucketKey, db, cache)
		targetMerkleTrees := bk.GetMerkleTrees()
		//更新叶子节点的dataHashes
		nodePath[0].dataHashes = make([][]byte, 0)
		segKeyInorder := make([]string, 0)
		bk.GetMerkleTrees().Range(func(key, value interface{}) bool {
			segKeyInorder = append(segKeyInorder, key.(string))
			return true
		})
		sort.Strings(segKeyInorder)
		for _, key := range segKeyInorder {
			targetMerkleTree, _ := targetMerkleTrees.Load(key)
			nodePath[0].dataHashes = append(nodePath[0].dataHashes, targetMerkleTree.(*mht.MerkleTree).GetRootHash())
		}
		//更新叶子节点的nodeHash,并将叶子节点存入leveldb
		nodePath[0].UpdateMGTNodeToDB(db, cache)
		//更新所有父节点的nodeHashes,并将父节点存入leveldb
		for i := 1; i < len(nodePath); i++ {
			if i == 1 {
				offset := len(bk.BucketKey) - len(nodePath[1].bucketKey)
				if offset == 1 {
					nodePath[1].dataHashes[bk.BucketKey[0]] = nodePath[0].nodeHash
				} else {
					nodePath[1].cachedDataHashes[bk.BucketKey[offset-1]] = nodePath[0].nodeHash
				}
			}
			// 一整条路径的值都会被修改为dirty，但是不会再重新计算哈希，因为这个操作会由batch调整的时候来做
			// 如果发现当前路径节点已经是dirty了，那这个节点再往上也一定会是dirty
			// 而且此函数进行时batch调整一定不会进行，因此也不会出现同一时刻batch调整将此处的dirty置为false的冲突情况
			if nodePath[i].isDirty {
				break
			} else {
				nodePath[i].isDirty = true
			}
		}
		// 桶的所有与mgtNode相关修改已经完毕，真正将桶锁释放
		bk.latch.Unlock()
	} else {
		//如果newBuckets中有多个bucket，则说明发生了分裂，MGT需要生长
		//分层依次生长,这样每次都只需要在一条路径上多扩展出一层mgtLeafNodes
		for _, newBuckets := range newBucketSs {
			var oldBucketKey []int
			// oldBucketKey应该由非连环分裂的桶决定，因为连环分裂桶的ld会因为下一层的更新而加一，连环分裂桶的bk也会因此比同一层的其他桶长一些
			if len(newBuckets[0].BucketKey) <= len(newBuckets[1].BucketKey) {
				oldBucketKey = GetOldBucketKey(newBuckets[0])
			} else {
				oldBucketKey = GetOldBucketKey(newBuckets[1])
			}
			//根据旧bucketKey,找到旧bucket所在的叶子节点
			nodePath = mgt.GetLeafNodeAndPath(oldBucketKey, db, cache)
			mgt.MGTGrow(oldBucketKey, nodePath, newBuckets, db, cache)
		}
		// 分裂桶的所有与mgtNode相关修改已经完毕，真正将桶锁释放
		for i, buckets := range newBucketSs {
			for j, bk := range buckets {
				if i != 0 && j == 0 { // 第一层往后每一层的第一个桶都是上一层分裂的那个桶，而上一层甚至更上层已经释放锁过了，因此跳过
					continue
				}
				bk.latch.Unlock()
			}
		}
	}
	return
}

// MGTGrow MGT生长,给定旧bucketKey和新的buckets,返回更新后的MGT
func (mgt *MGT) MGTGrow(oldBucketKey []int, nodePath []*MGTNode, newBuckets []*Bucket, db *leveldb.DB, cache *[]interface{}) *MGT {
	//为每个新的bucket创建叶子节点,并插入到leafNode的subNodes中
	subNodes := make([]*MGTNode, 0)
	commonLNKeyLength := 0
	if len(newBuckets[0].BucketKey) <= len(newBuckets[1].BucketKey) {
		commonLNKeyLength = len(newBuckets[0].BucketKey)
	} else {
		commonLNKeyLength = len(newBuckets[1].BucketKey)
	}
	for i := 0; i < len(newBuckets); i++ {
		newNode := NewMGTNode(nil, true, newBuckets[i], db, newBuckets[0].rdx, cache, commonLNKeyLength)
		subNodes = append(subNodes, newNode)
		// 新节点还是会去重新计算哈希的，这样就可以并发去重新计算节点，而不是等到batch调整的时候可能串行着去重新计算
		newNode.UpdateMGTNodeToDB(db, cache)
	}

	//更新hotnessList，更新叶子节点的访问频次，这一行是串行去做的
	mgt.UpdateHotnessList("split", util.IntArrayToString(oldBucketKey, mgt.rdx), 0, subNodes)

	//创建新的父节点
	newFatherNode := NewMGTNode(subNodes, false, nil, db, newBuckets[0].rdx, cache)
	newFatherNode.UpdateMGTNodeToDB(db, cache)

	//如果当前分裂的节点是缓存节点,则需要将其分裂出的子节点放入缓存叶子节点列表中,该节点放入缓存中间节点列表中
	if len(oldBucketKey) >= len(nodePath) {
		mgt.cachedLNMap.Delete(util.IntArrayToString(oldBucketKey, mgt.rdx))
		mgt.cachedINMap.Store(util.IntArrayToString(oldBucketKey, mgt.rdx), true)
		for _, node := range subNodes {
			mgt.cachedLNMap.Store(util.IntArrayToString(node.bucketKey, mgt.rdx), true)
		}
	}

	//更新父节点的child为新的父节点
	if len(nodePath) == 1 { // 此处根节点哈希已经成功更新，因此不需要标为dirty
		mgt.Root = newFatherNode
		return mgt
	}
	// 同一时刻一个桶只会有一个线程更新，因此对应的，这个mgtNode也只会被一个线程更新，因此此处不会被并发覆盖
	if len(nodePath[1].bucketKey)+1 == len(nodePath[0].bucketKey) {
		nodePath[1].subNodes[oldBucketKey[0]] = newFatherNode
		nodePath[1].dataHashes[oldBucketKey[0]] = newFatherNode.nodeHash
	} else {
		delta := len(oldBucketKey) - len(nodePath) + 1
		nodePath[1].cachedNodes[oldBucketKey[delta]] = newFatherNode
		nodePath[1].cachedDataHashes[oldBucketKey[delta]] = newFatherNode.nodeHash
	}
	if cache != nil {
		targetCache, _ := (*cache)[0].(*lru.Cache[string, *MGTNode])
		targetCache.Remove(string(nodePath[0].nodeHash))
	}
	if err := db.Delete(nodePath[0].nodeHash, nil); err != nil {
		panic(err)
	}
	newFatherNode.parent = nodePath[1]

	//更新所有父节点的nodeHash
	for i := 1; i < len(nodePath); i++ {
		// 同样的，一整条路径的值都会被修改为dirty，但是不会再重新计算哈希，因为这个操作会由batch调整的时候来做
		// 如果发现当前路径节点已经是dirty了，那这个节点再往上也一定会是dirty
		// 而且此函数进行时batch调整一定不会进行，因此也不会出现同一时刻batch调整将此处的dirty置为false的冲突情况
		if nodePath[i].isDirty {
			break
		} else {
			// 总有一个线程会将这个节点确实需要后续调整的脏节点的脏标识位置为true
			nodePath[i].isDirty = true
		}
	}
	return mgt
}

// UpdateNodeHash 根据子节点哈希和缓存子节点哈希计算当前节点哈希
func UpdateNodeHash(node *MGTNode) {
	nodeHash := make([]byte, 0)
	keys := make([]int, 0)
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

// UpdateHotnessList 访问频次统计
// 更新频次列表:
// op是操作类型，包括new,add,split，
// bk是待操作的叶节点bucketKey的string形式,hn是对应要增加的热度,一般为1
// 当op为split时，bk是旧叶子节点的bucketKey，subNodes是新生成的子叶子节点列表
func (mgt *MGT) UpdateHotnessList(op string, bk string, hn int, subNodes []*MGTNode) {
	mgt.updateLatch.Lock()
	defer mgt.updateLatch.Unlock()
	if op == "new" {
		mgt.hotnessList[bk] = hn
	} else if op == "add" {
		mgt.hotnessList[bk] = mgt.hotnessList[bk] + hn
	} else if op == "split" {
		// 此处所有mgtNode对应桶已经上写锁，因此mgtNode本身不会有任何新增修改，bucket.number的读取不会有任何因并发产生的问题
		originHot := mgt.hotnessList[bk] + hn
		recordSum := 0
		for _, node := range subNodes {
			recordSum += node.bucket.number
		}
		for _, node := range subNodes {
			subHotness := int(float64(originHot) * (float64(node.bucket.number) / float64(recordSum)))
			if subHotness != 0 {
				mgt.hotnessList[util.IntArrayToString(node.bucketKey, mgt.rdx)] = subHotness
			}
		}
		delete(mgt.hotnessList, bk)
	} else {
		fmt.Println("Error: no op!")
	}
}

// UpdateAccessLengthSum 更新叶子节点访问路径总长度
func (mgt *MGT) UpdateAccessLengthSum(accessPath int) {
	mgt.updateLatch.Lock()
	mgt.accessLength = mgt.accessLength + accessPath
	mgt.updateLatch.Unlock()
}

// IsNeedCacheAdjust 计算是否需要调整缓存
// a和b是计算阈值的两个参数
func (mgt *MGT) IsNeedCacheAdjust(bucketNum int, a float64, b float64) bool {
	//计算总访问频次
	accessNum := 0
	hotnessSlice := util.SortStringIntMapByInt(&mgt.hotnessList)
	for i := 0; i < len(hotnessSlice); i++ {
		accessNum = accessNum + hotnessSlice[i].GetValue()
	}
	//计算访问长度阈值
	threshold := (a*math.Log(float64(bucketNum)/float64(10))/math.Log(float64(mgt.rdx)) + b*math.Log(float64(bucketNum))/math.Log(float64(mgt.rdx))) * float64(accessNum)
	return float64(mgt.accessLength) > threshold
}

// CacheAdjust 调整缓存
func (mgt *MGT) CacheAdjust(db *leveldb.DB, cache *[]interface{}) {
	//第一步: 先将所有非叶子节点放回原处
	mgt.cachedINMap.Range(func(key, value interface{}) bool {
		INode := key.(string)
		//找到其所在的路径
		bkINode, _ := util.StringToIntArray(INode, mgt.rdx)
		//说明当前节点虽然是IN但是父亲节点也是IN，由subNode字段连接，不需要做额外cacheNode清空操作
		//只需要将父亲节点不是IN的IN节点移动到正确位置即可
		nodePath := mgt.GetInternalNodeAndPath(bkINode, db, cache)
		if len(nodePath[1].bucketKey)+1 == len(nodePath[0].bucketKey) {
			mgt.cachedINMap.Delete(INode)
			return true
		}
		//将其所在的缓存父节点(nodePath[1])相应的缓存位置空
		tempBK := bkINode[:len(bkINode)-len(nodePath[1].bucketKey)]
		nodePath[1].cachedNodes[tempBK[len(tempBK)-1]] = nil
		nodePath[1].cachedDataHashes[tempBK[len(tempBK)-1]] = nil
		for i := 1; i < len(nodePath); i++ {
			if !nodePath[i].isDirty {
				nodePath[i].isDirty = true
			} else {
				break
			}
		}
		//将该INode在cachedINMap中删除
		mgt.cachedINMap.Delete(INode)
		//将以INode为后缀的所有cachedLNMap中删除
		mgt.cachedLNMap.Range(func(key, value interface{}) bool {
			if strings.HasSuffix(key.(string), INode) {
				mgt.cachedLNMap.Delete(key.(string))
			}
			return true
		})
		//获取其父节点
		nodePathFather := mgt.GetInternalNodeAndPath(bkINode[1:], db, cache)
		//将其父节点的相应孩子位置为该节点
		nodePathFather[0].subNodes[bkINode[0]] = nodePath[0]
		nodePath[0].parent = nodePathFather[0]
		nodePathFather[0].dataHashes[bkINode[0]] = nodePath[0].nodeHash
		//nodePathFather[0]可能有多个孩子都被更新，因此为避免自底向上的多次更新，直接标记为脏
		for i := 0; i < len(nodePathFather); i++ {
			if !nodePathFather[i].isDirty {
				nodePathFather[i].isDirty = true
			} else {
				break
			}
		}
		return true
	})
	//将缓存的中间节点清空
	mgt.cachedINMap = sync.Map{}

	//第二步: 从hotnessList中依次选择最高的访问桶进行放置
	hotnessSlice := util.SortStringIntMapByInt(&mgt.hotnessList)
	for j := 0; j < len(hotnessSlice); j++ {
		// hotnessSlice[j].PrintKV()
		bucketKey, _ := util.StringToIntArray(hotnessSlice[j].GetKey(), mgt.rdx)
		//找到当前叶子节点及其路径
		nodePath := mgt.GetLeafNodeAndPath(bucketKey, db, cache)
		if !nodePath[0].isLeaf {
			fmt.Println("Not leaf!!!!")
		}
		if len(nodePath) <= 2 {
			//说明该叶子节点是根节点或者该叶子节点在根节点的缓存目录中,已经没有缓存优化空间
			//fmt.Println("已是叶节点", hotnessSlice[j].GetKey(), "的最佳位置")
			//mgt.PrintCachedPath(nodePath)
			continue
		}
		//从nodePath的最后一个开始,倒着看是否能够放置
		identI := len(bucketKey) - 1
		for i := len(nodePath) - 1; i > 1; i-- {
			//如果当前节点的第bucketKey[identI]个缓存位为空,则放置
			if nodePath[i].cachedDataHashes[bucketKey[identI]] == nil {
				//1.放置当前节点(nodePath[0])
				nodePath[i].cachedNodes[bucketKey[identI]] = nodePath[0]
				nodePath[0].parent = nodePath[i]
				nodePath[i].cachedDataHashes[bucketKey[identI]] = nodePath[0].nodeHash
				//2.如果nodePath是缓存路径，则将nodePath[1]相应缓存位清空
				//更新nodePath[1]及以后的所有节点，由于可能有多个孩子都被更新，因此为避免自底向上的多次更新，直接标记为脏
				for k := 1; k < len(nodePath); k++ {
					if !nodePath[k].isDirty {
						nodePath[k].isDirty = true
					} else {
						break
					}
				}
				tempBK := bucketKey[:len(bucketKey)-len(nodePath[1].bucketKey)]
				delPos := len(tempBK) - 1
				if val, ok := mgt.cachedLNMap.Load(util.IntArrayToString(bucketKey, mgt.rdx)); ok && val.(bool) {
					nodePath[1].cachedNodes[tempBK[delPos]] = nil
					nodePath[1].cachedDataHashes[tempBK[delPos]] = nil
				} else {
					//将该叶子节点放入缓存Map中
					mgt.cachedLNMap.Store(hotnessSlice[j].GetKey(), true)
				}
				break
			} else { //如果当前节点的第bucketKey[identI]个缓存位不为空
				//比较缓存节点与当前叶节点的热度,如果当前节点更热,则替换,并将原缓存节点放回原处,否则继续看下一位置
				cachedLN := nodePath[i].GetCachedNode(bucketKey[identI], db, mgt.rdx, cache)
				//待插入节点更热
				if mgt.hotnessList[util.IntArrayToString(cachedLN.bucketKey, mgt.rdx)] == 0 || mgt.hotnessList[util.IntArrayToString(cachedLN.bucketKey, mgt.rdx)] < mgt.hotnessList[util.IntArrayToString(nodePath[0].bucketKey, mgt.rdx)] {
					//1.先将cachedLN放回原处
					//在cachedMap中删除
					mgt.cachedLNMap.Delete(util.IntArrayToString(cachedLN.bucketKey, mgt.rdx))
					//找到cachedLN的父节点
					npFather := mgt.GetLeafNodeAndPath(cachedLN.bucketKey[1:], db, cache)
					//将cachedLN放入其父节点的子节点中
					npFather[0].subNodes[cachedLN.bucketKey[0]] = cachedLN
					cachedLN.parent = npFather[0]
					npFather[0].dataHashes[cachedLN.bucketKey[0]] = cachedLN.nodeHash
					//2.更新np_father上所有节点，由于可能有多个孩子都被更新，因此为避免自底向上的多次更新，直接标记为脏
					for k := 0; k < len(npFather); k++ {
						if !npFather[k].isDirty {
							npFather[k].isDirty = true
						} else {
							break
						}
					}
					//3.再将当前节点放入nodePath[i]的缓存目录中
					nodePath[i].cachedNodes[bucketKey[identI]] = nodePath[0]
					nodePath[0].parent = nodePath[i]
					nodePath[i].cachedDataHashes[bucketKey[identI]] = nodePath[0].nodeHash
					//更新nodePath[1]及以后的所有节点，由于可能有多个孩子都被更新，因此为避免自底向上的多次更新，直接标记为脏
					for k := 1; k < len(nodePath); k++ {
						if !nodePath[k].isDirty {
							nodePath[k].isDirty = true
						} else {
							break
						}
					}
					//4.如果nodePath是缓存路径，则将nodePath[1]相应缓存位清空
					tempBK := bucketKey[:len(bucketKey)-len(nodePath[1].bucketKey)]
					delPos := len(tempBK) - 1
					if val, ok := mgt.cachedLNMap.Load(util.IntArrayToString(bucketKey, mgt.rdx)); ok && val.(bool) {
						nodePath[1].cachedNodes[tempBK[delPos]] = nil
						nodePath[1].cachedDataHashes[tempBK[delPos]] = nil
					} else {
						//将该节点放入缓存Map中
						mgt.cachedLNMap.Store(hotnessSlice[j].GetKey(), true)
					}
					break
				} else {
					//原缓存节点更热
					identI--
				}
			}
		}
	}
	//第三步: 清空统计列表
	mgt.hotnessList = make(map[string]int)
	mgt.accessLength = 0
}

// PrintCachedPath 打印缓存更新后的缓存路径
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

// PrintCachedMaps 打印缓存情况
func (mgt *MGT) PrintCachedMaps() {
	cachedINMap := make(map[string]bool)
	cachedLNMap := make(map[string]bool)
	mgt.cachedINMap.Range(func(key, value interface{}) bool {
		cachedINMap[key.(string)] = value.(bool)
		return true
	})
	mgt.cachedLNMap.Range(func(key, value interface{}) bool {
		cachedLNMap[key.(string)] = value.(bool)
		return true
	})
	fmt.Println("cachedLNMap:", cachedLNMap)
	fmt.Println("cachedINMap:", cachedINMap)
}

// PrintHotnessList 打印访问频次列表和总访问次数
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

// PrintMGT 打印MGT
func (mgt *MGT) PrintMGT(db *leveldb.DB, cache *[]interface{}) {
	fmt.Printf("打印MGT-------------------------------------------------------------------------------------------\n")
	if mgt == nil {
		return
	}
	//递归打印MGT
	mgt.latch.RLock() //mgt结构将不会更新，只会将未从磁盘中完全加载的结构从磁盘更新到内存结构中
	fmt.Printf("MGTRootHash: %x\n", mgt.mgtRootHash)
	mgt.PrintMGTNode(mgt.GetRoot(db), 0, db, cache)
	mgt.latch.RUnlock()
}

// PrintMGTNode 递归打印MGT
func (mgt *MGT) PrintMGTNode(node *MGTNode, level int, db *leveldb.DB, cache *[]interface{}) {
	//跳转到此函数时mgt已经加写锁
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
			mgt.PrintMGTNode(node.GetSubNode(i, db, mgt.rdx, cache), level+1, db, cache)
			mgt.PrintMGTNode(node.GetSubNode(i, db, mgt.rdx, cache), level+1, db, cache)
		}
	}
	for i := 0; i < len(node.cachedDataHashes); i++ {
		if !node.isLeaf && node.cachedDataHashes[i] != nil {
			mgt.PrintMGTNode(node.GetCachedNode(i, db, mgt.rdx, cache), level+1, db, cache)
		}
	}
}

type MGTProof struct {
	level    int    //哈希值所在的层数
	dataHash []byte //哈希值
}

func (mgtProof *MGTProof) GetSizeOf() uint {
	return util.SIZEOFINT + uint(len(mgtProof.dataHash))*util.SIZEOFBYTE
}

// GetProof 给定bucketKey，返回它的mgtRootHash和mgtProof，不存在则返回nil
func (mgt *MGT) GetProof(bucketKey []int, db *leveldb.DB, cache *[]interface{}) ([]byte, []MGTProof) {
	//跳转到此函数时已对MGT加锁
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
		for j := 0; j < len(nodePath[i].cachedDataHashes); j++ {
			mgtProof = append(mgtProof, MGTProof{i, nodePath[i].cachedDataHashes[j]})
		}
	}
	return mgt.Root.nodeHash, mgtProof
}

// ComputeMGTRootHash 给定segRootHash和mgtProof，返回由它们计算得到的mgtRootHash
func ComputeMGTRootHash(segRootHash []byte, mgtProof []MGTProof) []byte {
	//遍历mgtProof中前segNum个元素，如果segRootHash不存在，则返回nil，否则计算得到第0个node的nodeHash
	//同样遍历第i层的所有元素，如果第i-1层的nodeHash不在其中，则返回nil，否则计算得到第i层node的nodeHash
	isSRHExist := false
	nodeHash0 := segRootHash
	nodeHash1 := make([]byte, 0)
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

// PrintMGTProof 打印mgtProof
func PrintMGTProof(mgtProof []MGTProof) {
	for i := 0; i < len(mgtProof); i++ {
		fmt.Printf("[%d,%s]\n", mgtProof[i].level, hex.EncodeToString(mgtProof[i].dataHash))
	}
}

type SeMGT struct {
	Rdx         int             //radix of bucket key, decide the number of sub-nodes
	MgtRootHash []byte          // hash of this MGT, equals to the hash of the root node hash
	CachedLNMap map[string]bool //当前被缓存在中间节点的叶子节点，存在bool为true，不存在就直接没有
	CachedINMap map[string]bool //当前被缓存在中间节点的叶子节点，存在bool为true，不存在就直接没有
}

func SerializeMGT(mgt *MGT) []byte {
	seCachedLNMap := make(map[string]bool)
	seCachedINMap := make(map[string]bool)
	mgt.cachedLNMap.Range(func(key, value interface{}) bool {
		seCachedLNMap[key.(string)] = value.(bool)
		return true
	})
	mgt.cachedINMap.Range(func(key, value interface{}) bool {
		seCachedINMap[key.(string)] = value.(bool)
		return true
	})
	seMGT := &SeMGT{mgt.rdx, mgt.mgtRootHash, seCachedLNMap, seCachedINMap}
	jsonMGT, err := json.Marshal(seMGT)
	if err != nil {
		fmt.Printf("SerializeMGT error: %v\n", err)
		return nil
	}
	return jsonMGT
}

func DeserializeMGT(data []byte) (*MGT, error) {
	var seMGT SeMGT
	if err := json.Unmarshal(data, &seMGT); err != nil {
		fmt.Printf("DeserializeMGT error: %v\n", err)
		return nil, err
	}
	mgt := &MGT{seMGT.Rdx, nil, seMGT.MgtRootHash, sync.Map{}, sync.Map{}, make(map[string]int), 0, sync.RWMutex{}, sync.Mutex{}}
	for key, value := range seMGT.CachedLNMap {
		mgt.cachedLNMap.Store(key, value)
	}
	for key, value := range seMGT.CachedINMap {
		mgt.cachedINMap.Store(key, value)
	}
	return mgt, nil
}

type SeMGTNode struct {
	NodeHash         []byte   // hash of this node, consisting of the hash of its children
	DataHashes       [][]byte // hashes of data elements, computed from subNodes, is used for indexing children nodes in leveldb
	CachedDataHashes [][]byte // hash of cached data elements, computed from cached subNodes
	IsLeaf           bool     // whether this node is a leaf node
	BucketKey        []int    // bucketKey related to this leaf node,is used for indexing bucket in leveldb
}

func SerializeMGTNode(node *MGTNode) []byte {
	seMGTNode := &SeMGTNode{node.nodeHash, node.dataHashes, node.cachedDataHashes, node.isLeaf, node.bucketKey}
	if jsonMGTNode, err := json.Marshal(seMGTNode); err != nil {
		fmt.Printf("SerializeMGTNode error: %v\n", err)
		return nil
	} else {
		return jsonMGTNode
	}
}

func DeserializeMGTNode(data []byte, rdx int) (*MGTNode, error) {
	var seMGTNode SeMGTNode
	if err := json.Unmarshal(data, &seMGTNode); err != nil {
		fmt.Printf("DeserializeMGTNode error: %v\n", err)
		return nil, err
	}
	subNodes := make([]*MGTNode, rdx)
	cachedNodes := make([]*MGTNode, rdx)
	if seMGTNode.IsLeaf {
		return &MGTNode{seMGTNode.NodeHash, nil, subNodes, seMGTNode.DataHashes, cachedNodes,
			seMGTNode.CachedDataHashes, seMGTNode.IsLeaf, false, nil, seMGTNode.BucketKey,
			sync.RWMutex{}, nil, nil}, nil
	}
	mgtNode := &MGTNode{seMGTNode.NodeHash, nil, subNodes, seMGTNode.DataHashes, cachedNodes,
		seMGTNode.CachedDataHashes, seMGTNode.IsLeaf, false, nil, seMGTNode.BucketKey,
		sync.RWMutex{}, make([]sync.RWMutex, 0), make([]sync.RWMutex, 0)}
	for i := 0; i < rdx; i++ {
		mgtNode.subNodesLatch = append(mgtNode.subNodesLatch, sync.RWMutex{})
		mgtNode.cachedNodesLatch = append(mgtNode.cachedNodesLatch, sync.RWMutex{})
	}
	return mgtNode, nil
}
