package mbt

import (
	"MEHT/util"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"sync"
)

type MBTNode struct {
	nodeHash [32]byte
	name     []byte

	parent     *MBTNode
	subNodes   []*MBTNode
	dataHashes [][32]byte

	isLeaf   bool
	isDirty  bool
	bucket   []util.KVPair
	num      int // num of kvPairs in bucket
	toDelMap map[string]map[string]int

	latch         sync.RWMutex
	subNodesLatch []sync.RWMutex
}

func NewMBTNode(name []byte, subNodes []*MBTNode, dataHashes [][32]byte, isLeaf bool, db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) (ret *MBTNode) {
	//由于MBT结构固定，因此此函数只会在MBt初始化时被调用，因此此时dataHashes一定为空，此时nodeHash一定仅包含name
	if isLeaf {
		ret = &MBTNode{[32]byte{}, name, nil, subNodes, dataHashes, isLeaf, false, make([]util.KVPair, 0),
			0, make(map[string]map[string]int), sync.RWMutex{}, make([]sync.RWMutex, 0)}
	} else {
		ret = &MBTNode{[32]byte{}, name, nil, subNodes, dataHashes, isLeaf, false, nil, -1,
			make(map[string]map[string]int), sync.RWMutex{}, make([]sync.RWMutex, 0)}
	}
	copy(ret.name[:], name)
	for _, node := range ret.subNodes {
		node.parent = ret
		ret.subNodesLatch = append(ret.subNodesLatch, sync.RWMutex{})
	}
	if cache != nil {
		cache.Add(string(ret.nodeHash[:]), ret)
	} else {
		if err := db.Put(ret.nodeHash[:], SerializeMBTNode(ret), nil); err != nil {
			log.Fatal("Insert MBTNode to DB error:", err)
		}
	}
	return
}

func (mbtNode *MBTNode) GetName() string {
	return string(mbtNode.name)
}

func (mbtNode *MBTNode) GetSubNode(index int, db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) *MBTNode {
	if mbtNode.subNodes[index] == nil && mbtNode.subNodesLatch[index].TryLock() {
		if mbtNode.subNodes[index] != nil {
			mbtNode.subNodesLatch[index].Unlock()
			return mbtNode.subNodes[index]
		}
		var node *MBTNode
		var ok bool
		if cache != nil {
			if node, ok = cache.Get(string(mbtNode.dataHashes[index][:])); ok {
				mbtNode.subNodes[index] = node
				node.parent = mbtNode
			}
		}
		if !ok {
			if nodeString, err := db.Get(mbtNode.dataHashes[index][:], nil); err == nil {
				node, _ = DeserializeMBTNode(nodeString)
				mbtNode.subNodes[index] = node
				node.parent = mbtNode
			}
		}
		mbtNode.subNodesLatch[index].Unlock()
	}
	for mbtNode.subNodes[index] == nil {
	}
	return mbtNode.subNodes[index]
}

func (mbtNode *MBTNode) UpdateMBTNodeHash(db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) {
	if cache != nil { //删除旧值
		cache.Remove(string(mbtNode.nodeHash[:]))
	}
	if err := db.Delete(mbtNode.nodeHash[:], nil); err != nil {
		fmt.Println("Error in UpdateMBTNodeHash: ", err)
	}
	nodeHash := make([]byte, len(mbtNode.name))
	copy(nodeHash, mbtNode.name)
	if mbtNode.isLeaf {
		dataHash := make([]byte, 0)
		for _, kv := range mbtNode.bucket {
			dataHash = append(dataHash, []byte(kv.GetValue())...)
		}
		//hash_ := sha256.Sum256(dataHash)
		mbtNode.dataHashes[0] = sha256.Sum256(dataHash)
	}
	for _, hash := range mbtNode.dataHashes {
		nodeHash = append(nodeHash, hash[:]...)
	}
	mbtNode.nodeHash = sha256.Sum256(nodeHash)
	if cache != nil { //存入新值
		cache.Add(string(mbtNode.nodeHash[:]), mbtNode)
	} else {
		if err := db.Put(mbtNode.nodeHash[:], SerializeMBTNode(mbtNode), nil); err != nil {
			log.Fatal("Insert MBTNode to DB error:", err)
		}
	}
}

type SeMBTNode struct {
	NodeHash   [32]byte
	Name       []byte
	DataHashes [][32]byte
	IsLeaf     bool
	Bucket     []util.SeKVPair
}

func SerializeMBTNode(node *MBTNode) []byte {
	Bucket := make([]util.SeKVPair, 0)
	for _, bk := range node.bucket {
		Bucket = append(Bucket, util.SeKVPair{Key: bk.GetKey(), Value: bk.GetValue()})
	}
	seMBTNode := &SeMBTNode{node.nodeHash, node.name, node.dataHashes, node.isLeaf, Bucket}
	if jsonMBTNode, err := json.Marshal(seMBTNode); err != nil {
		fmt.Printf("SerializeMBTNode error: %v\n", err)
		return nil
	} else {
		return jsonMBTNode
	}
}

func DeserializeMBTNode(data []byte) (*MBTNode, error) {
	var seMBTNode SeMBTNode
	if err := json.Unmarshal(data, &seMBTNode); err != nil {
		fmt.Printf("DeserializeMBTNode error: %v\n", err)
		return nil, err
	}
	bucket := make([]util.KVPair, 0)
	for _, bk := range seMBTNode.Bucket {
		bucket = append(bucket, *util.NewKVPair(bk.Key, bk.Value))
	}
	if seMBTNode.IsLeaf {
		return &MBTNode{seMBTNode.NodeHash, seMBTNode.Name, nil, nil, seMBTNode.DataHashes, true, false,
			bucket, len(seMBTNode.Bucket), make(map[string]map[string]int), sync.RWMutex{}, nil}, nil
	} else {
		ret := &MBTNode{seMBTNode.NodeHash, seMBTNode.Name, nil, make([]*MBTNode, len(seMBTNode.DataHashes)), seMBTNode.DataHashes, false,
			false, nil, len(seMBTNode.Bucket), make(map[string]map[string]int), sync.RWMutex{}, make([]sync.RWMutex, 0)}
		for i := 0; i < len(seMBTNode.DataHashes); i++ {
			ret.subNodesLatch = append(ret.subNodesLatch, sync.RWMutex{})
		}
		return ret, nil
	}
}
