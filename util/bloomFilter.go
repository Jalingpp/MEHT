package util

import (
	"github.com/bits-and-blooms/bitset"
	"github.com/bits-and-blooms/bloom/v3"
)

const BFCapacity uint = 1000000

type BF struct {
	f          *bloom.BloomFilter
	windowSize uint
	//id int
}

func NewBF() BF {
	return BF{f: bloom.NewWithEstimates(BFCapacity, 0.01)}
}

func (bf *BF) Add(data []byte) {
	bf.f.Add(data)
}

func (bf *BF) Test(data []byte) bool {
	return bf.f.Test(data)
}

func (bf *BF) GobEncode() ([]byte, error) {
	return bf.f.GobEncode()
}

func (bf *BF) GobDecode(data []byte) error {
	return bf.f.GobDecode(data)
}

func (bf *BF) BitSet() *bitset.BitSet {
	return bf.f.BitSet()
}

func (bf *BF) Len() uint {
	return bf.f.BitSet().Len()
}

func (bf *BF) Cap() uint {
	return BFCapacity
}

func (bf *BF) WindowSize() uint {
	return bf.windowSize
}

//func (bf *BF) ID() int {
//	return bf.id
//}
//
//func (bf *BF) SetID(id int) {
//	bf.id = id
//}

func (bf *BF) Merge(other *BF) {
	if err := bf.f.Merge(other.f); err != nil {
		panic(err)
	}
}

func (bf *BF) Copy() BF {
	return BF{f: bf.f.Copy()}
}

func (bf *BF) Clear() {
	bf.f.ClearAll()
}
