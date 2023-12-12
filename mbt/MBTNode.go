package mbt

import (
	"MEHT/util"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"strings"
	"sync"
)

type MBTNode struct {
	nodeHash []byte
	name     []byte

	subNodes   []*MBTNode
	dataHashes [][]byte

	isLeaf bool
	bucket []util.KVPair
	num    int // num of kvPairs in bucket

	latch       sync.RWMutex
	updateLatch sync.Mutex
}

func NewMBTNode(name []byte, subNodes []*MBTNode, dataHashes [][]byte, isLeaf bool, db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) (ret *MBTNode) {
	//由于MBT结构固定，因此此函数只会在MBt初始化时被调用，因此此时dataHashes一定为空，此时nodeHash一定仅包含name
	if isLeaf {
		ret = &MBTNode{name, name, subNodes, dataHashes, isLeaf, make([]util.KVPair, 0),
			0, sync.RWMutex{}, sync.Mutex{}}
	} else {
		ret = &MBTNode{name, name, subNodes, dataHashes, isLeaf, nil, -1,
			sync.RWMutex{}, sync.Mutex{}}
	}
	if cache != nil {
		cache.Add(string(ret.nodeHash), ret)
	} else {
		if err := db.Put(ret.nodeHash, SerializeMBTNode(ret), nil); err != nil {
			log.Fatal("Insert MBTNode to DB error:", err)
		}
	}
	return
}

func (mbtNode *MBTNode) GetSubnode(index int, db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) *MBTNode {
	mbtNode.updateLatch.Lock()
	defer mbtNode.updateLatch.Unlock()
	if mbtNode.subNodes[index] == nil {
		var node *MBTNode
		var ok bool
		if cache != nil {
			if node, ok = cache.Get(string(mbtNode.dataHashes[index])); ok {
				mbtNode.subNodes[index] = node
			}
		}
		if !ok {
			if nodeString, err := db.Get(mbtNode.dataHashes[index], nil); err == nil {
				node, _ = DeserializeMBTNode(nodeString)
				mbtNode.subNodes[index] = node
			}
		}
	}
	return mbtNode.subNodes[index]
}

func UpdateMBTNodeHash(node *MBTNode, dirtyNodeIdx int, db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) {
	if cache != nil { //删除旧值
		cache.Remove(string(node.nodeHash))
	}
	db.Delete(node.nodeHash, nil)
	nodeHash := make([]byte, len(node.name))
	copy(nodeHash, node.name)
	if node.isLeaf {
		dataHash := make([]byte, 0)
		for _, kv := range node.bucket {
			dataHash = append(dataHash, []byte(kv.GetValue())...)
		}
		dataHash_ := sha256.Sum256(dataHash)
		node.dataHashes[0] = dataHash_[:]
	} else {
		node.dataHashes[dirtyNodeIdx] = node.subNodes[dirtyNodeIdx].nodeHash
	}
	for _, hash := range node.dataHashes {
		nodeHash = append(nodeHash, hash...)
	}
	newHash := sha256.Sum256(nodeHash)
	node.nodeHash = newHash[:]
	if cache != nil { //存入新值
		cache.Add(string(node.nodeHash), node)
	} else {
		if err := db.Put(node.nodeHash, SerializeMBTNode(node), nil); err != nil {
			log.Fatal("Insert MBTNode to DB error:", err)
		}
	}
}

type SeMBTNode struct {
	NodeHash   []byte
	Name       []byte
	DataHashes string
	IsLeaf     bool
	Bucket     []util.KVPair
}

func SerializeMBTNode(node *MBTNode) []byte {
	dataHashString := ""
	if len(node.dataHashes) > 0 {
		dataHashString += hex.EncodeToString(node.dataHashes[0])
		for _, hash := range node.dataHashes[1:] {
			dataHashString += "," + hex.EncodeToString(hash)
		}
	}
	if len(strings.Split(dataHashString, ",")) == 5 {
		fmt.Println("QQQQ")
	}
	if string(node.name) == "Root" {
		fmt.Println("ZZZ")
	}
	seMBTNode := &SeMBTNode{node.nodeHash, node.name, dataHashString, node.isLeaf, node.bucket}
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
	if string(seMBTNode.Name) == "Root" {
		fmt.Println("YYY")
	}
	dataHashes := make([][]byte, 0)
	dataHashStrings := strings.Split(seMBTNode.DataHashes, ",")
	for i := 0; i < len(dataHashStrings); i++ {
		dataHash, _ := hex.DecodeString(dataHashStrings[i])
		dataHashes = append(dataHashes, dataHash)
	}
	if seMBTNode.IsLeaf {
		return &MBTNode{seMBTNode.NodeHash, seMBTNode.Name, nil, dataHashes, true, seMBTNode.Bucket, len(seMBTNode.Bucket), sync.RWMutex{}, sync.Mutex{}}, nil
	} else {
		return &MBTNode{seMBTNode.NodeHash, seMBTNode.Name, make([]*MBTNode, len(dataHashes)), dataHashes, false, nil, len(seMBTNode.Bucket), sync.RWMutex{}, sync.Mutex{}}, nil
	}
}
