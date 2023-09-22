package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"fmt"
	"strconv"
)

type Bucket struct {
	bucketKey string // bucket key, initial nil, related to ld and kvpair.key

	ld  int // local depth, initial zero
	rdx int // rdx, initial 2

	capacity int // capacity of the bucket, initial given
	number   int // number of data objects in the bucket, initial zero

	segNum      int                        // number of segment bits in the bucket, initial given
	segments    map[string][]*util.KVPair  // segments: data objects in each segment
	merkleTrees map[string]*mht.MerkleTree // merkle trees: one for each segment
}

// NewBucket creates a new Bucket object
func NewBucket(ld int, rdx int, capacity int, segNum int) *Bucket {
	return &Bucket{"", ld, rdx, capacity, 0, segNum, make(map[string][]*util.KVPair, 0), make(map[string]*mht.MerkleTree, 0)}
}

// GetBucketKey returns the bucket key of the Bucket
func (b *Bucket) GetBucketKey() string {
	return b.bucketKey
}

// GetLD returns the local depth of the Bucket
func (b *Bucket) GetLD() int {
	return b.ld
}

// GetRdx returns the rdx of the Bucket
func (b *Bucket) GetRdx() int {
	return b.rdx
}

// GetCapacity returns the capacity of the Bucket
func (b *Bucket) GetCapacity() int {
	return b.capacity
}

// GetNumber returns the number of data objects in the Bucket
func (b *Bucket) GetNumber() int {
	return b.number
}

// GetSegNum returns the number of segments in the Bucket
func (b *Bucket) GetSegNum() int {
	return b.segNum
}

// GetSegments returns the segments of the Bucket
func (b *Bucket) GetSegments() map[string][]*util.KVPair {
	return b.segments
}

// GetMerkleTrees returns the merkle trees of the Bucket
func (b *Bucket) GetMerkleTrees() map[string]*mht.MerkleTree {
	return b.merkleTrees
}

// SetBucketKey sets the bucket key of the Bucket
func (b *Bucket) SetBucketKey(bucketKey string) {
	b.bucketKey = bucketKey
}

// SetLD sets the local depth of the Bucket
func (b *Bucket) SetLD(ld int) {
	b.ld = ld
}

// SetRdx sets the rdx of the Bucket
func (b *Bucket) SetRdx(rdx int) {
	b.rdx = rdx
}

// SetCapacity sets the capacity of the Bucket
func (b *Bucket) SetCapacity(capacity int) {
	b.capacity = capacity
}

// SetNumber sets the number of data objects in the Bucket
func (b *Bucket) SetNumber(number int) {
	b.number = number
}

// SetSegNum sets the number of segments in the Bucket
func (b *Bucket) SetSegNum(segNum int) {
	b.segNum = segNum
}

// 给定一个key，返回该key所在的segment map key
func (b *Bucket) GetSegment(key string) string {
	return key[:b.segNum]
}

// 给定一个key, 判断它是否在该bucket中
func (b *Bucket) IsInBucket(key string) bool {
	segkey := b.GetSegment(key)
	kvpairs := b.segments[segkey]
	for _, kvpair := range kvpairs {
		if kvpair.GetKey() == key {
			return true
		}
	}
	return false
}

// 给定一个key, 返回它在该bucket中的value
func (b *Bucket) GetValue(key string) string {
	segkey := b.GetSegment(key)
	kvpairs := b.segments[segkey]
	//判断是否在bucket中,在则返回value,不在则返回空字符串
	for _, kvpair := range kvpairs {
		if kvpair.GetKey() == key {
			return kvpair.GetValue()
		}
	}
	return ""
}

// 给定一个key, 返回它所在的segment及其index, 如果不在, 返回-1
func (b *Bucket) GetIndex(key string) (string, int) {
	segkey := b.GetSegment(key)
	kvpairs := b.segments[segkey]
	for i, kvpair := range kvpairs {
		if kvpair.GetKey() == key {
			return segkey, i
		}
	}
	return "", -1
}

// 给定一个KVPair, 将它插入到该bucket中,返回插入后的bucket指针,若发生分裂,返回分裂后的rdx个bucket指针
func (b *Bucket) Insert(kvpair util.KVPair) []*Bucket {
	buckets := make([]*Bucket, 0)
	//判断是否在bucket中,在则返回所在的segment及其index,不在则返回-1
	segkey, index := b.GetIndex(kvpair.GetKey())
	if index != -1 {
		//在bucket中,修改value
		b.segments[segkey][index].SetValue(kvpair.GetValue())
		//更新bucket中对应segment的merkle tree
		newSegHashRoot := b.merkleTrees[segkey].UpdateRoot(index, []byte(kvpair.GetValue()))
		buckets = append(buckets, b)
		//输出segkey, index, newSegHashRoot
		fmt.Printf("当前bucket已存在该key,更新value\n")
		fmt.Printf("segkey: %s, index: %d, newSegHashRoot: %x\n", segkey, index, newSegHashRoot)
		updateProof := b.merkleTrees[segkey].GetProof(index)
		//输出更新后的value和proof
		fmt.Printf("更新后的value: %s\n", kvpair.GetValue())
		fmt.Printf("更新后的proof: %x\n", updateProof)
		return buckets
	} else {
		//获得kvpair的key的segment
		segkey := b.GetSegment(kvpair.GetKey())
		//判断segment是否存在,不存在则创建,同时创建merkleTree
		if _, ok := b.segments[segkey]; !ok {
			b.segments[segkey] = make([]*util.KVPair, 0)
		}
		//判断bucket是否已满
		if b.number < b.capacity {
			//未满,插入到对应的segment中
			b.segments[segkey] = append(b.segments[segkey], &kvpair)
			index := len(b.segments[segkey]) - 1
			// fmt.Printf("index: %d\n", index)
			b.number++
			//若该segment对应的merkle tree不存在,则创建,否则插入value到merkle tree中
			if _, ok := b.merkleTrees[segkey]; !ok {
				b.merkleTrees[segkey] = mht.NewEmptyMerkleTree()
			}
			//插入value到merkle tree中,返回新的根哈希
			newSegHashRoot := b.merkleTrees[segkey].InsertData([]byte(kvpair.GetValue()))
			buckets = append(buckets, b)
			//输出segkey, index, newSegHashRoot
			fmt.Printf("当前bucket不存在该key,已插入key-value\n")
			fmt.Printf("segkey: %s, index: %d, newSegHashRoot: %x\n", segkey, index, newSegHashRoot)
			// b.merkleTrees[segkey].PrintTree()
			updateProof := b.merkleTrees[segkey].GetProof(index)
			//输出插入的value和proof
			fmt.Printf("插入的value: %s\n", kvpair.GetValue())
			fmt.Printf("proof: %x\n", updateProof)
			return buckets
		} else {
			//已满,分裂成rdx个bucket
			buckets = append(buckets, b.SplitBucket()...)
			//为所有bucket构建merkle tree
			for _, bucket := range buckets {
				segments := bucket.GetSegments()
				for segkey, kvpairs := range segments {
					//将kvpair转化为[][]byte
					datas := make([][]byte, 0)
					for _, kvpair := range kvpairs {
						datas = append(datas, []byte(kvpair.GetValue()))
					}
					bucket.merkleTrees[segkey] = mht.NewMerkleTree(datas)
				}
			}
			//判断key应该插入到哪个bucket中
			ikey := kvpair.GetKey()
			bk := ikey[len(ikey)-b.ld:][0] - '0'
			fmt.Printf("当前bucket已满,已分裂成%d个bucket,该key应该插入到第%d个bucket中\n", b.rdx, bk)
			buckets[bk].Insert(kvpair)
			return buckets
		}
	}
}

// SplitBucket 分裂bucket为rdx个bucket,并将原bucket中的数据重新分配到rdx个bucket中,返回rdx个bucket的指针
func (b *Bucket) SplitBucket() []*Bucket {
	buckets := make([]*Bucket, 0)
	b.SetLD(b.GetLD() + 1)
	originBkey := b.GetBucketKey()
	b.SetBucketKey("0" + originBkey)
	b.number = 0
	bsegments := b.GetSegments()
	b.segments = make(map[string][]*util.KVPair, 0)
	b.merkleTrees = make(map[string]*mht.MerkleTree, 0)
	buckets = append(buckets, b)
	//创建rdx-1个新bucket
	for i := 0; i < b.rdx-1; i++ {
		newBucket := NewBucket(b.ld, b.rdx, b.capacity, b.segNum)
		newBucket.SetBucketKey(strconv.Itoa(i+1) + originBkey)
		buckets = append(buckets, newBucket)
	}
	//获取原bucket中所有数据对象
	for _, value := range bsegments {
		kvpairs := value
		for _, kvpair := range kvpairs {
			k := kvpair.GetKey()
			//获取key的倒数第ld位
			bk := k[len(k)-b.ld:][0] - '0'
			//将数据对象插入到对应的bucket中
			buckets[bk].Insert(*kvpair)
		}
	}
	return buckets
}

// 给定一个key, 返回它的value,所在segment的根哈希,存在的proof,若不存在,返回空字符串,空字符串,空数组
func (b *Bucket) GetProof(key string) (string, []byte, [][]byte) {
	segkey, index := b.GetIndex(key)
	if index != -1 {
		value := b.segments[segkey][index].GetValue()
		segHashRoot := b.merkleTrees[segkey].GetRootHash()
		proof := b.merkleTrees[segkey].GetProof(index)
		return value, segHashRoot, proof
	}
	return "", []byte(""), [][]byte{}
}

// 打印bucket中的所有数据对象
func (b *Bucket) PrintBucket() {
	//打印ld,rdx,capacity,number,segNum
	fmt.Printf("ld: %d,rdx: %d,capacity: %d,number: %d,segNum: %d\n", b.ld, b.rdx, b.capacity, b.number, b.segNum)
	//打印segments
	for segkey, value := range b.GetSegments() {
		fmt.Printf("segkey: %s\n", segkey)
		kvpairs := value
		for _, kvpair := range kvpairs {
			fmt.Printf("%s:%s\n", kvpair.GetKey(), kvpair.GetValue())
		}
	}
	//打印所有merkle tree
	for segkey, merkleTree := range b.GetMerkleTrees() {
		fmt.Printf("Merkle Tree of Each Segement\n")
		fmt.Printf("segkey: %s\n", segkey)
		merkleTree.PrintTree()
	}
}

//拷贝到main.go中测试
// func main() {

// 	//测试Bucket
// 	//创建一个bucket
// 	bucket := meht.NewBucket(nil,0, 2, 2, 1) //ld,rdx,capacity,segNum
// 	//创建4个KVPair
// 	kvpair1 := util.NewKVPair("0000", "value1")
// 	kvpair2 := util.NewKVPair("0001", "value2")
// 	kvpair3 := util.NewKVPair("0010", "value3")
// 	kvpair4 := util.NewKVPair("0011", "value4")
// 	//插入4个KVPair
// 	buckets1 := bucket.Insert(*kvpair1)
// 	buckets2 := bucket.Insert(*kvpair2)
// 	buckets3 := bucket.Insert(*kvpair3)
// 	buckets4 := bucket.Insert(*kvpair4)

// 	//打印buckets1中所有的bucket
// 	fmt.Printf("buckets1中所有的bucket\n")
// 	for _, bucket := range buckets1 {
// 		bucket.PrintBucket()
// 	}

// 	//打印buckets2中所有的bucket
// 	fmt.Printf("buckets2中所有的bucket\n")
// 	for _, bucket := range buckets2 {
// 		bucket.PrintBucket()
// 	}

// 	//打印buckets3中所有的bucket
// 	fmt.Printf("buckets3中所有的bucket\n")
// 	for _, bucket := range buckets3 {
// 		bucket.PrintBucket()
// 	}

// 	//打印buckets4中所有的bucket
// 	fmt.Printf("buckets4中所有的bucket\n")
// 	for _, bucket := range buckets4 {
// 		bucket.PrintBucket()
// 	}
// }
