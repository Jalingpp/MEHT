package mbt

import (
	"MEHT/util"
	"encoding/hex"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"strings"
	"sync"
)

type MBTNode struct {
	nodeHash []byte

	subNodes   []*MBTNode
	dataHashes [][]byte

	isLeaf    bool
	bucket    []util.KVPair
	bucketKey []int

	latch       sync.RWMutex
	updateLatch sync.Mutex
}

func (mbtNode *MBTNode) GetSubnode(index int, db *leveldb.DB, rdx int, cache *lru.Cache[string, *MBTNode]) *MBTNode {
	if mbtNode.subNodes[index] == nil && mbtNode.updateLatch.TryLock() {
		var node *MBTNode
		var ok bool
		if cache != nil {
			if node, ok = cache.Get(string(mbtNode.dataHashes[index])); ok {
				mbtNode.subNodes[index] = node
			}
		}
		if !ok {
			if nodeString, err := db.Get(mbtNode.dataHashes[index], nil); err == nil {
				node, _ = DeserializeMBTNode(nodeString, rdx)
				mbtNode.subNodes[index] = node
			}
		}
		mbtNode.updateLatch.Unlock()
	}
	for mbtNode.subNodes[index] == nil {
	}
	return mbtNode.subNodes[index]
}

type SeMBTNode struct {
	NodeHash   []byte
	DataHashes string
	IsLeaf     bool
	Bucket     []util.KVPair
	BucketKey  []int
}

func SerializeMBTNode(node *MBTNode) []byte {
	dataHashString := ""
	if len(node.dataHashes) > 0 {
		dataHashString += hex.EncodeToString(node.dataHashes[0])
		for _, hash := range node.dataHashes[1:] {
			dataHashString += hex.EncodeToString(hash) + ","
		}
	}
	seMBTNode := &SeMBTNode{node.nodeHash, dataHashString, node.isLeaf, node.bucket, node.bucketKey}
	if jsonMBTNode, err := json.Marshal(seMBTNode); err != nil {
		fmt.Printf("SerializeMBTNode error: %v\n", err)
		return nil
	} else {
		return jsonMBTNode
	}
}

func DeserializeMBTNode(data []byte, rdx int) (*MBTNode, error) {
	var seMBTNode SeMBTNode
	if err := json.Unmarshal(data, &seMBTNode); err != nil {
		fmt.Printf("DeserializeMBTNode error: %v\n", err)
		return nil, err
	}
	dataHashes := make([][]byte, 0)
	dataHashStrings := strings.Split(seMBTNode.DataHashes, ",")
	for i := 0; i < len(dataHashStrings); i++ {
		dataHash, _ := hex.DecodeString(dataHashStrings[i])
		dataHashes = append(dataHashes, dataHash)
	}
	if seMBTNode.IsLeaf {
		return &MBTNode{seMBTNode.NodeHash, nil, dataHashes, true, seMBTNode.Bucket, seMBTNode.BucketKey, sync.RWMutex{}, sync.Mutex{}}, nil
	} else {
		return &MBTNode{seMBTNode.NodeHash, make([]*MBTNode, rdx), dataHashes, false, nil, seMBTNode.BucketKey, sync.RWMutex{}, sync.Mutex{}}, nil
	}
}
